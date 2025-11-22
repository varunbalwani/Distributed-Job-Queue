# Distributed Job Queue (Prototype) - Option B

This repository is a complete prototype of a small distributed job queue and worker system
using Go and SQLite (file-backed). It implements Option B: events are persisted so multiple
API instances can replay events; an in-memory broadcaster forwards events to SSE clients.

## Quick start (local)

1. Build:
   ```bash
   go build -o bin/api ./cmd/api
   go build -o bin/worker ./cmd/worker
   ```

2. Run:
   ```bash
   ./bin/api &
   ./bin/worker &
   ```

3. Open dashboard:
   http://localhost:8080/dashboard

4. Submit job:
   ```bash
   curl -X POST http://localhost:8080/jobs \
     -H 'Content-Type: application/json' \
     -d '{"payload": {"task":"hello"}, "idempotency_key":"abc"}'
   ```

## Notes

- Persistence: SQLite file `jobs.db`.
- Events persisted to `events` table (so new API instances can replay).
- SSE endpoint `/events` streams live updates.
- Quotas: max 5 concurrent running jobs per tenant; max 10 new jobs/minute.
- Lease duration: 30s, max retries: 3 (per job).
