package job

import (
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID             string `json:"id"`
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Payload        string `json:"payload"`
	Status         string `json:"status"`
	Retries        int    `json:"retries"`
	MaxRetries     int    `json:"max_retries"`
	LeaseUntil     int64  `json:"lease_until"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      int64  `json:"updated_at"`
}

type Event struct {
	ID        int64  `json:"id"`
	JobID     string `json:"job_id"`
	Type      string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt int64  `json:"created_at"`
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository { return &Repository{db: db} }

var ErrNotFound = errors.New("not found")

func (r *Repository) CreateIfNotExist(j *Job) (*Job, error) {
	// check idempotency
	if j.IdempotencyKey != "" {
		row := r.db.QueryRow("SELECT id, status, payload, retries, max_retries, lease_until, created_at, updated_at FROM jobs WHERE tenant_id=$1 AND idempotency_key=$2", j.TenantID, j.IdempotencyKey)
		var id string
		if err := row.Scan(&id, &j.Status, &j.Payload, &j.Retries, &j.MaxRetries, &j.LeaseUntil, &j.CreatedAt, &j.UpdatedAt); err == nil {
			j.ID = id
			return j, nil
		}
	}
	now := time.Now().Unix()
	j.ID = uuid.NewString()
	j.Status = "pending"
	j.CreatedAt = now
	j.UpdatedAt = now
	_, err := r.db.Exec("INSERT INTO jobs(id, tenant_id, idempotency_key, payload, status, retries, max_retries, lease_until, created_at, updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
		j.ID, j.TenantID, j.IdempotencyKey, j.Payload, j.Status, j.Retries, j.MaxRetries, j.LeaseUntil, j.CreatedAt, j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Repository) GetByID(id string) (*Job, error) {
	row := r.db.QueryRow("SELECT id, tenant_id, idempotency_key, payload, status, retries, max_retries, lease_until, created_at, updated_at FROM jobs WHERE id=$1", id)
	var j Job
	if err := row.Scan(&j.ID, &j.TenantID, &j.IdempotencyKey, &j.Payload, &j.Status, &j.Retries, &j.MaxRetries, &j.LeaseUntil, &j.CreatedAt, &j.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &j, nil
}

func (r *Repository) CountRunningByTenant(tenant string) (int, error) {
	row := r.db.QueryRow("SELECT COUNT(1) FROM jobs WHERE tenant_id=$1 AND status='running'", tenant)
	var c int
	if err := row.Scan(&c); err != nil {
		return 0, err
	}
	return c, nil
}

func (r *Repository) CountCreatedInLastMin(tenant string) (int, error) {
	since := time.Now().Add(-1 * time.Minute).Unix()
	row := r.db.QueryRow("SELECT COUNT(1) FROM jobs WHERE tenant_id=$1 AND created_at>=$2", tenant, since)
	var c int
	if err := row.Scan(&c); err != nil {
		return 0, err
	}
	return c, nil
}

func (r *Repository) LeaseNext(leaseSeconds int) (*Job, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // Safe to call even after Commit succeeds

	now := time.Now().Unix()
	leaseUntil := now + int64(leaseSeconds)

	// Use FOR UPDATE to lock the row and prevent other workers from selecting it
	// SKIP LOCKED ensures we skip rows that are already locked by other transactions
	row := tx.QueryRow(`
		SELECT id, tenant_id, idempotency_key, payload, retries, max_retries 
		FROM jobs 
		WHERE status='pending' 
		ORDER BY created_at 
		LIMIT 1 
		FOR UPDATE SKIP LOCKED
	`)

	var j Job
	if err := row.Scan(&j.ID, &j.TenantID, &j.IdempotencyKey, &j.Payload, &j.Retries, &j.MaxRetries); err != nil {
		if err == sql.ErrNoRows {
			// No pending jobs available
			return nil, nil
		}
		return nil, err
	}

	// Update the job status - row is already locked, so this is guaranteed to succeed
	result, err := tx.Exec(`
		UPDATE jobs 
		SET status='running', lease_until=$1, updated_at=$2 
		WHERE id=$3 AND status='pending'
	`, leaseUntil, now, j.ID)
	if err != nil {
		return nil, err
	}

	// Verify that exactly one row was updated
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rowsAffected == 0 {
		// This should never happen due to FOR UPDATE, but it's a safety check
		return nil, errors.New("job was already leased by another worker")
	}

	// Commit the transaction - this releases the row lock
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	// Populate the returned job object with the new values
	j.Status = "running"
	j.LeaseUntil = leaseUntil
	j.UpdatedAt = now
	return &j, nil
}

func (r *Repository) MarkDone(id string) error {
	now := time.Now().Unix()
	_, err := r.db.Exec("UPDATE jobs SET status='done', updated_at=$1 WHERE id=$2", now, id)
	return err
}

func (r *Repository) MarkFailedOrRetry(id string) (bool, error) {
	row := r.db.QueryRow("SELECT retries, max_retries FROM jobs WHERE id=$1", id)
	var retries, maxR int
	if err := row.Scan(&retries, &maxR); err != nil {
		return false, err
	}
	now := time.Now().Unix()
	if retries+1 >= maxR {
		_, err := r.db.Exec("UPDATE jobs SET status='failed_dlq', retries=$1, updated_at=$2 WHERE id=$3", retries+1, now, id)
		return true, err
	}
	_, err := r.db.Exec("UPDATE jobs SET status='pending', retries=$1, updated_at=$2 WHERE id=$3", retries+1, now, id)
	return false, err
}

func (r *Repository) RequeueExpiredLeases() ([]string, error) {
	now := time.Now().Unix()
	// Find expired jobs first
	rows, err := r.db.Query("SELECT id FROM jobs WHERE status='running' AND lease_until<$1", now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Update them
	// In a real app, do this in a transaction or batch
	for _, id := range ids {
		_, _ = r.db.Exec("UPDATE jobs SET status='pending', updated_at=$1 WHERE id=$2", now, id)
	}
	return ids, nil
}

// Event persistence (Option B)
func (r *Repository) SaveEvent(jobID, eventType, payload string) error {
	now := time.Now().Unix()
	_, err := r.db.Exec("INSERT INTO events(job_id, event_type, payload, created_at) VALUES($1,$2,$3,$4)", jobID, eventType, payload, now)
	return err
}

func (r *Repository) GetRecentEvents(limit int) ([]Event, error) {
	rows, err := r.db.Query("SELECT id, job_id, event_type, payload, created_at FROM events ORDER BY created_at DESC LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var evs []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.JobID, &e.Type, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		evs = append(evs, e)
	}
	return evs, nil
}

func (r *Repository) GetEventsAfterID(lastID int64) ([]Event, error) {
	rows, err := r.db.Query("SELECT id, job_id, event_type, payload, created_at FROM events WHERE id > $1 ORDER BY id ASC", lastID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var evs []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.JobID, &e.Type, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		evs = append(evs, e)
	}
	return evs, nil
}

func (r *Repository) Counts() (map[string]int, error) {
	q := `SELECT
        SUM(CASE WHEN status IN ('pending') THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running,
        SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
        SUM(CASE WHEN status = 'failed_dlq' THEN 1 ELSE 0 END) as failed
      FROM jobs`
	row := r.db.QueryRow(q)
	var pending, running, done, failed int
	if err := row.Scan(&pending, &running, &done, &failed); err != nil {
		return nil, err
	}
	return map[string]int{"pending": pending, "running": running, "done": done, "failed": failed}, nil
}
