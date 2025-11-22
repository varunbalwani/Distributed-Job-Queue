package tests

import (
	"database/sql"
	"os"
	"testing"

	_ "modernc.org/sqlite"
)

func TestCreateAndGet(t *testing.T) {
	os.Remove("test.db")
	db, err := sql.Open("sqlite", "file:test.db?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, tenant_id TEXT, idempotency_key TEXT, payload TEXT, status TEXT, retries INTEGER, max_retries INTEGER, lease_until INTEGER, created_at INTEGER, updated_at INTEGER)`); err != nil {
		t.Fatal(err)
	}
}
