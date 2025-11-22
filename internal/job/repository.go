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
		row := r.db.QueryRow("SELECT id, status, payload, retries, max_retries, lease_until, created_at, updated_at FROM jobs WHERE tenant_id=? AND idempotency_key=?", j.TenantID, j.IdempotencyKey)
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
	_, err := r.db.Exec("INSERT INTO jobs(id, tenant_id, idempotency_key, payload, status, retries, max_retries, lease_until, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
		j.ID, j.TenantID, j.IdempotencyKey, j.Payload, j.Status, j.Retries, j.MaxRetries, j.LeaseUntil, j.CreatedAt, j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (r *Repository) GetByID(id string) (*Job, error) {
	row := r.db.QueryRow("SELECT id, tenant_id, idempotency_key, payload, status, retries, max_retries, lease_until, created_at, updated_at FROM jobs WHERE id=?", id)
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
	row := r.db.QueryRow("SELECT COUNT(1) FROM jobs WHERE tenant_id=? AND status='running'", tenant)
	var c int
	if err := row.Scan(&c); err != nil {
		return 0, err
	}
	return c, nil
}

func (r *Repository) CountCreatedInLastMin(tenant string) (int, error) {
	since := time.Now().Add(-1 * time.Minute).Unix()
	row := r.db.QueryRow("SELECT COUNT(1) FROM jobs WHERE tenant_id=? AND created_at>=?", tenant, since)
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
	defer tx.Rollback()

	now := time.Now().Unix()
	leaseUntil := now + int64(leaseSeconds)

	row := tx.QueryRow("SELECT id, tenant_id, idempotency_key, payload, retries, max_retries FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 1")
	var j Job
	if err := row.Scan(&j.ID, &j.TenantID, &j.IdempotencyKey, &j.Payload, &j.Retries, &j.MaxRetries); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if _, err := tx.Exec("UPDATE jobs SET status='running', lease_until=?, updated_at=? WHERE id=?", leaseUntil, now, j.ID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	j.Status = "running"
	j.LeaseUntil = leaseUntil
	j.UpdatedAt = now
	return &j, nil
}

func (r *Repository) MarkDone(id string) error {
	now := time.Now().Unix()
	_, err := r.db.Exec("UPDATE jobs SET status='done', updated_at=? WHERE id=?", now, id)
	return err
}

func (r *Repository) MarkFailedOrRetry(id string) (bool, error) {
	row := r.db.QueryRow("SELECT retries, max_retries FROM jobs WHERE id=?", id)
	var retries, maxR int
	if err := row.Scan(&retries, &maxR); err != nil {
		return false, err
	}
	now := time.Now().Unix()
	if retries+1 >= maxR {
		_, err := r.db.Exec("UPDATE jobs SET status='failed_dlq', retries=?, updated_at=? WHERE id=?", retries+1, now, id)
		return true, err
	}
	_, err := r.db.Exec("UPDATE jobs SET status='pending', retries=?, updated_at=? WHERE id=?", retries+1, now, id)
	return false, err
}

func (r *Repository) RequeueExpiredLeases() ([]string, error) {
	now := time.Now().Unix()
	// Find expired jobs first
	rows, err := r.db.Query("SELECT id FROM jobs WHERE status='running' AND lease_until<?", now)
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
		_, _ = r.db.Exec("UPDATE jobs SET status='pending', updated_at=? WHERE id=?", now, id)
	}
	return ids, nil
}

// Event persistence (Option B)
func (r *Repository) SaveEvent(jobID, eventType, payload string) error {
	now := time.Now().Unix()
	_, err := r.db.Exec("INSERT INTO events(job_id, event_type, payload, created_at) VALUES(?,?,?,?)", jobID, eventType, payload, now)
	return err
}

func (r *Repository) GetRecentEvents(limit int) ([]Event, error) {
	rows, err := r.db.Query("SELECT id, job_id, event_type, payload, created_at FROM events ORDER BY created_at DESC LIMIT ?", limit)
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
	rows, err := r.db.Query("SELECT id, job_id, event_type, payload, created_at FROM events WHERE id > ? ORDER BY id ASC", lastID)
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
