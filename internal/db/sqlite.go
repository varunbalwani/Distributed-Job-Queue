package db

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

func Open(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_foreign_keys=1", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	// create tables if not exists
	schema := `
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        tenant_id TEXT,
        idempotency_key TEXT,
        payload TEXT,
        status TEXT,
        retries INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        lease_until INTEGER DEFAULT 0,
        created_at INTEGER,
        updated_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_idemp ON jobs(tenant_id, idempotency_key);

    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        event_type TEXT,
        payload TEXT,
        created_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);
    `
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}
