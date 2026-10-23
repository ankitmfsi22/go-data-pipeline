# Go Data Processing Pipeline

[![CI](https://github.com/ankitmfsi22/go-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ankitmfsi22/go-data-pipeline/actions/workflows/ci.yml)

A concurrent data processing pipeline in Go. It reads data from multiple sources (CSV, JSON, HTTP APIs or rows sent in the request), validates and transforms the records, computes aggregations and saves the results to SQLite and to JSON/CSV files. Jobs are created and tracked through a REST API, and service metrics are exposed for Prometheus.

Built with the Go standard library only, no web frameworks. The only external dependency is the SQLite driver.

## Features

- **Pipeline stages:** ingest → validate → transform → aggregate → export, connected by channels
- **Concurrency:** one goroutine per source, worker pools (fan-out / fan-in) for validate and transform, one goroutine for aggregation
- **Job manager:** each job runs in the background with its own context, so it can be cancelled or time out; a limit on how many jobs run at once (extra jobs wait as `pending`)
- **Error handling:** a bad record doesn't stop the job. It's saved with its file, line, stage and reason. A failed job saves nothing partial
- **Progress:** live counters per job (records read, passed, failed, records/sec, average time per stage)
- **REST API:** create, list, get, progress, results (paginated), errors, cancel, delete, plus file upload
- **Observability:** request ID, request logging, panic recovery and a Prometheus `/metrics` endpoint, all built with the standard library
- **Quality:** unit and integration tests with the race detector, golangci-lint, and CI on every push
- **Security:** optional API key (`X-API-Key`), rate limit on POST requests, secure headers, upload content check, file paths limited to allowed folders (`DATA_DIRS`)

## Folder Structure

```
go-data-pipeline/
├── .github/workflows/
│   └── ci.yml           # CI: format check, lint, build and tests on every push
├── cmd/
│   └── server/
│       └── main.go      # Entry point: wires everything, starts the server, graceful shutdown
├── internal/
│   ├── config/          # Reads settings from environment variables
│   ├── logger/          # Structured logging (log/slog)
│   ├── server/          # HTTP server, /healthz and /readyz, middleware chain, shutdown
│   ├── api/             # REST handlers, file upload and middleware (request ID, logging, metrics, recover)
│   ├── job/             # Job manager (run, cancel, timeout, concurrency limit) and progress tracker
│   ├── pipeline/        # Validate, transform and aggregate stages, worker pool, Plan that wires all stages
│   ├── source/          # Ingest stage: CSV, JSON, HTTP and inline readers
│   ├── export/          # Export stage: SQLite, JSON and CSV (atomic file writes)
│   ├── store/           # SQLite: jobs, records, summaries and errors
│   ├── metrics/         # Counters and gauges in Prometheus text format
│   └── domain/          # Shared types: Record, Spec, Job, Progress, RecordError
├── test/                # Integration tests: full jobs through the real API and database
├── examples/            # Sample job spec, sample data and Postman collection
├── docs/                # Architecture diagram
├── .githooks/           # pre-commit (fmt, vet, lint, tests) and commit-msg checks
├── .env.example         # Sample environment variables
├── prometheus.yml       # Local Prometheus config (scrapes /metrics every 5s)
├── .golangci.yml        # Linter rules
├── Makefile             # Common commands
└── go.mod               # Go module
```

## How It Works

1. `main.go` loads the config, opens the SQLite database, creates the metrics, the job manager and the API, and starts the HTTP server in a goroutine.
2. Every request passes through middleware: it gets a request ID, is logged, is counted in metrics, and a panic becomes a `500` instead of crashing the server.
3. `POST /api/v1/pipelines` receives a job spec (JSON). The spec is checked and every stage is built before any data is read, so a wrong rule returns `400` right away.
4. The job manager saves the job as `pending`, starts it in its own goroutine and returns `202` with the job ID. The job waits for a free slot if too many jobs are running.
5. The ingest stage reads every source in its own goroutine and sends the records into one channel.
6. Records go through the validate and transform stages. Each stage runs N workers in parallel (fan-out) that write to one output channel (fan-in).
7. Records that fail a rule or a conversion go to an error channel with the stage and reason. The job keeps running.
8. The aggregate stage (one goroutine) computes count, sum, avg, min and max, optionally grouped by a field.
9. The export stage saves the records and summary to SQLite in one transaction, and to JSON/CSV files if the spec asks for them.
10. The job ends as `completed`, `failed` (missing file, timeout) or `cancelled`. Rejected records are saved for `GET /errors`.
11. Every record updates the job's progress and the service metrics. Prometheus reads `/metrics` every 5 seconds.
12. On Ctrl+C the server stops taking new requests, running jobs are cancelled, and the database is closed.

## Run

```bash
cp .env.example .env   # set API_KEY here (leave empty to turn auth off)
make hooks             # enable git hooks (run once)
make run               # start the server on :8080
make test              # run all tests with the race detector
make lint              # run golangci-lint
make fmt               # format the code
make build             # build the binary into bin/
```

```bash
curl http://localhost:8080/healthz
curl -X POST localhost:8080/api/v1/pipelines -H "X-API-Key: $API_KEY" -d @examples/covid-job.json
```

Postman: import `examples/postman_collection.json` and set the `apiKey` variable.

## API

Base URL: `http://localhost:8080`

| Method | Endpoint | Description | Success |
|---|---|---|---|
| GET | `/healthz` | Is the server alive | 200 |
| GET | `/readyz` | Can the server take requests | 200 |
| POST | `/api/v1/files` | Upload a CSV/JSON file (form-data, key `file`), returns its `location` | 201 |
| POST | `/api/v1/pipelines` | Create a job from a spec, returns the job ID | 202 |
| GET | `/api/v1/pipelines` | List all jobs | 200 |
| GET | `/api/v1/pipelines/{id}` | Job status, error, times and spec | 200 |
| GET | `/api/v1/pipelines/{id}/progress` | Live counters: read, passed, failed, records/sec, stage latency | 200 |
| GET | `/api/v1/pipelines/{id}/results?limit=100&offset=0` | Summary and saved records (paginated) | 200 |
| GET | `/api/v1/pipelines/{id}/errors` | Rejected records with file, line, stage and reason | 200 |
| PATCH | `/api/v1/pipelines/{id}/cancel` | Stop a running job | 202 |
| DELETE | `/api/v1/pipelines/{id}` | Delete a finished job and its data | 204 |
| GET | `/metrics` | Service metrics in Prometheus format | 200 |

Errors always return `{"error": "message"}`: `400` invalid spec, body or file path outside `DATA_DIRS`, `401` missing or wrong `X-API-Key`, `404` unknown job, `409` action not allowed in the current state, `429` too many requests (with `Retry-After`).

### Example job

```json
{
  "name": "covid-and-prices",
  "sources": [
    { "type": "csv", "location": "internal/source/testdata/covid.csv" },
    { "type": "json", "location": "internal/source/testdata/prices.json" },
    { "type": "inline", "data": [{ "country": "Japan", "cases": "700" }] }
  ],
  "validation": [{ "field": "cases", "rule": "min", "value": 0 }],
  "transformations": [
    { "field": "country", "op": "lowercase" },
    { "field": "cases", "op": "to_number" }
  ],
  "aggregations": [
    { "op": "count" },
    { "op": "sum", "field": "cases", "group_by": "country" },
    { "op": "avg", "field": "price_usd" }
  ],
  "exports": [
    { "type": "db" },
    { "type": "json", "path": "output/report.json" }
  ],
  "options": { "workers": 3, "timeout": "1m" }
}
```

| Part | Supported values |
|---|---|
| Sources | `csv`, `json` (file path), `http` (URL), `inline` (rows in `data`) |
| Validation rules | `required`, `number`, `min`, `max` |
| Transformations | `trim`, `lowercase`, `uppercase`, `to_number` |
| Aggregations | `count`, `sum`, `avg`, `min`, `max`, with optional `group_by` |
| Exports | `db`, `json`, `csv` |
| Options | `workers`, `buffer_size`, `timeout` (defaults come from config) |

## Metrics (Prometheus)

`GET /metrics` returns the service metrics in the Prometheus text format. Run Prometheus locally:

```bash
prometheus --config.file=prometheus.yml --storage.tsdb.path=/tmp/prom-data
```

Open `http://localhost:9090`. `Status → Targets` should show `go-data-pipeline` as **UP**.

| Metric | Meaning |
|---|---|
| `pipeline_jobs_created_total` | Jobs created |
| `pipeline_jobs_running` | Jobs running right now |
| `pipeline_jobs_finished_total{status}` | Finished jobs by status |
| `pipeline_records_total{stage}` | Records handled per stage |
| `pipeline_records_failed_total{stage}` | Records rejected per stage |
| `pipeline_stage_duration_seconds{stage}` | Time per record per stage (`_sum` / `_count`) |
| `http_requests_total{route,status}` | API requests by route and status |
| `http_request_duration_seconds{route}` | API response time by route |
| `go_goroutines` | Goroutines running |

Useful queries:

```
rate(pipeline_records_total[1m])                                              # records per second
pipeline_stage_duration_seconds_sum / pipeline_stage_duration_seconds_count   # average time per record
http_requests_total{status=~"4.."}                                            # client errors
```

## Configuration

All settings come from environment variables and have defaults, so the service runs with no setup.

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Server address |
| `DATABASE_URL` | `pipeline.db` | SQLite file |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `PIPELINE_DEFAULT_WORKERS` | `4` | Workers per stage when a job doesn't set it |
| `PIPELINE_DEFAULT_BUFFER` | `100` | Channel buffer size |
| `PIPELINE_DEFAULT_TIMEOUT` | `10m` | Job timeout |
| `PIPELINE_MAX_CONCURRENT_JOBS` | `5` | Jobs running at the same time; the rest wait |
| `SHUTDOWN_TIMEOUT` | `20s` | Time allowed for graceful shutdown |
| `API_KEY` | empty | Required `X-API-Key` value for `/api/*`; empty turns auth off |
| `RATE_LIMIT_PER_MINUTE` | `60` | POST requests allowed per IP per minute; `0` turns it off |
| `DATA_DIRS` | `data,examples,internal/source/testdata` | Folders that csv/json sources can read from |
| `UPLOAD_DIR` | `data/uploads` | Where uploaded files are saved |

Example: `PIPELINE_MAX_CONCURRENT_JOBS=1 LOG_FORMAT=text make run`

## Testing

```bash
make test                # all tests with the race detector
go test -short ./...     # skip the large-file integration test
go test -v ./test/       # integration tests only
```

- **Unit tests** for every package: sources, rules, transforms, worker pool, aggregation, export, store, job manager, API, middleware and metrics.
- **Integration tests** (`test/`) start the real server with a temporary database and run full jobs over HTTP: sample data, an HTTP source, a 20,000-row file, concurrent jobs, cancel, timeout and delete.
- **CI** (GitHub Actions) runs the format check, lint, build and all tests on every push to `main` and on pull requests.

## High Level Diagram
<img width="692" height="539" alt="Screenshot 2026-10-12 at 8 32 59 PM" src="https://github.com/user-attachments/assets/b05f6523-5f08-44a1-9ce8-34007d4218cc" />
