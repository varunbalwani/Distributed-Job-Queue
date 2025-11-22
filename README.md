# Distributed Job Queue

A production-ready distributed job queue and worker system built with Go and PostgreSQL. This system implements event sourcing with Server-Sent Events (SSE) for real-time updates, supporting multiple API instances with event replay capabilities.

## 🚀 Live Demo

**Deployment**: [https://distributed-job-queue-o59s.onrender.com/](https://distributed-job-queue-o59s.onrender.com/)

## Features

- ✅ **PostgreSQL Backend**: Production-grade persistence with ACID guarantees
- ✅ **Event Sourcing**: All job state changes persisted as events for replay
- ✅ **Real-time Updates**: SSE endpoint streams live job updates to dashboard
- ✅ **Multi-tenant Support**: Isolated job queues per tenant
- ✅ **Rate Limiting**: Configurable quotas per tenant
- ✅ **Automatic Retries**: Failed jobs automatically retry with exponential backoff
- ✅ **Dead Letter Queue**: Failed jobs moved to DLQ after max retries
- ✅ **Lease-based Processing**: Prevents duplicate processing with automatic lease recovery
- ✅ **Health Checks**: Worker service health monitoring
- ✅ **Beautiful Dashboard**: Real-time job monitoring UI

## Quick Start (Local)

### Prerequisites

- Go 1.21+
- PostgreSQL 16+ (or use Docker)
- Docker & Docker Compose (optional)

### Option 1: Using Docker Compose (Recommended)

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Access the dashboard**:
   ```
   http://localhost:8080/
   ```

### Option 2: Manual Setup

1. **Set up PostgreSQL**:
   ```bash
   # Start PostgreSQL (using Docker)
   docker-compose up -d postgres
   
   # Or use your own PostgreSQL instance
   ```

2. **Configure environment**:
   ```bash
   # Create .env file
   echo "DATABASE_URL=postgresql://jobqueue:jobqueue_dev@localhost:5432/jobqueue?sslmode=disable" > .env
   ```

3. **Build the services**:
   ```bash
   go build -o bin/api ./cmd/api
   go build -o bin/worker ./cmd/worker
   ```

4. **Run the services**:
   ```bash
   # Terminal 1 - Start worker
   ./bin/worker
   
   # Terminal 2 - Start API server
   ./bin/api
   ```

5. **Access the dashboard**:
   ```
   http://localhost:8080/
   ```

## API Usage

### Submit a Job

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant": "demo",
    "payload": {"task": "process_data", "value": 123},
    "idempotency_key": "unique-key-123"
  }'
```

### Get Job Status

```bash
curl http://localhost:8080/jobs/{job_id}
```

### Real-time Events

```bash
curl -N http://localhost:8080/events
```

### Metrics

```bash
curl http://localhost:8080/metrics
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://jobqueue:jobqueue_dev@localhost:5432/jobqueue?sslmode=disable` | PostgreSQL connection string |
| `WORKER_URL` | `http://localhost:10000/health` | Worker health check endpoint |

### System Parameters

- **Lease Duration**: 5 seconds
- **Max Retries**: 3 per job
- **Concurrent Jobs per Tenant**: 5
- **Rate Limit**: 10 jobs/minute per tenant
- **Worker Health Port**: 10000
- **API Server Port**: 8080

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌────────────┐
│   Client    │─────▶│  API Server  │─────▶│ PostgreSQL │
│  (Browser)  │◀─────│   (Port 8080)│◀─────│            │
└─────────────┘ SSE  └──────────────┘      └────────────┘
                            │                      ▲
                            │ Health Check         │
                            ▼                      │
                     ┌──────────────┐              │
                     │    Worker    │──────────────┘
                     │ (Port 10000) │   Process Jobs
                     └──────────────┘
```

### Components

1. **API Server** (`cmd/api`): Handles HTTP requests, manages SSE connections, broadcasts events
2. **Worker** (`cmd/worker`): Leases and processes jobs, handles retries and failures
3. **PostgreSQL**: Stores jobs, events, and system state
4. **Dashboard** (`ui/index.html`): Real-time job monitoring interface

## Database Schema

### Jobs Table
- `id`: Unique job identifier (UUID)
- `tenant_id`: Tenant identifier for multi-tenancy
- `idempotency_key`: Prevents duplicate job submission
- `payload`: Job data (JSONB)
- `status`: Current job state (pending/running/done/failed_dlq)
- `retries`: Current retry count
- `max_retries`: Maximum retry attempts
- `lease_until`: Lease expiration timestamp
- `created_at`, `updated_at`: Timestamps

### Events Table
- `id`: Auto-incrementing event ID
- `job_id`: Associated job ID
- `type`: Event type (submitted/started/done/failed/retry/timeout/failed_dlq)
- `payload`: Event data (JSONB)
- `created_at`: Event timestamp

## Development

### Running Tests

```bash
go test ./...
```

### Project Structure

```
.
├── cmd/
│   ├── api/          # API server entry point
│   └── worker/       # Worker service entry point
├── internal/
│   ├── db/           # Database connection
│   ├── job/          # Job service, repository, processor
│   └── server/       # HTTP handlers and routing
├── ui/               # Dashboard frontend
├── docker-compose.yml
└── README.md
```

## Deployment

The application is deployed on Render with:
- API server as a web service
- Worker as a background worker
- PostgreSQL as a managed database

**Live URL**: [https://distributed-job-queue-o59s.onrender.com/](https://distributed-job-queue-o59s.onrender.com/)

## License

MIT
