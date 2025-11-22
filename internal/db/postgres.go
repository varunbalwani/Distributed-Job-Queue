package db

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func Open(connString string) (*sql.DB, error) {
	// If connString is empty, use environment variable
	if connString == "" {
		connString = os.Getenv("DATABASE_URL")
		if connString == "" {
			return nil, fmt.Errorf("database connection string required (provide as argument or set DATABASE_URL environment variable)")
		}
	}

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	// Create tables if not exists
	schema := `
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        tenant_id TEXT,
        idempotency_key TEXT,
        payload TEXT,
        status TEXT,
        retries INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        lease_until BIGINT DEFAULT 0,
        created_at BIGINT,
        updated_at BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_idemp ON jobs(tenant_id, idempotency_key);

    CREATE TABLE IF NOT EXISTS events (
        id BIGSERIAL PRIMARY KEY,
        job_id TEXT,
        event_type TEXT,
        payload TEXT,
        created_at BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
    `
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}
