# Go Data Processing Pipeline

A concurrent data processing pipeline in Go. It reads data from multiple sources (CSV, JSON, HTTP APIs), validates and transforms the records, computes aggregations and saves the results. Jobs are managed through a REST API.

Built with the Go standard library only, no web frameworks.

## Note : Work in progress. This README is updated daily as new features are added

## Folder Structure

```
go-data-pipeline/
├── cmd/
│   ├── server/
│   │   └── main.go      # Entry point: starts the server and handles graceful shutdown
│   └── ingest-demo/
│       └── main.go      # Demo: runs ingest -> validate -> transform on sample data
├── internal/
│   ├── config/          # Reads settings from environment variables
│   ├── logger/          # Structured logging (log/slog)
│   ├── server/          # HTTP server with /healthz and /readyz
│   ├── domain/          # Shared types: Record, job Spec and RecordError
│   ├── source/          # Ingest stage: CSV, JSON and HTTP readers
│   └── pipeline/        # Validate and transform stages, worker pool
├── docs/                # Architecture diagram
├── .golangci.yml        # Linter rules
├── Makefile             # Common commands
└── go.mod               # Go module
```

## How It Works

1. `main.go` loads the config, creates the logger and starts the HTTP server in a goroutine.
2. The server exposes `/healthz` (is the app alive?) and `/readyz` (can it take requests?).
3. On Ctrl+C, the server stops taking new requests, lets running requests finish, then exits.
4. The ingest stage reads every source (CSV file, JSON file, HTTP API) in its own goroutine and sends the records into one channel.
5. Records go through the validate and transform stages. Each stage runs N workers in parallel (fan-out) that write to one output channel (fan-in).
6. Records that fail a rule or a conversion are sent to an error channel with the stage and reason. The job keeps running.

## Run

```bash
make run      # start the server
make test     # run tests
make lint     # run linter
```

```bash
go run ./cmd/ingest-demo   # read the sample files and print the records
curl http://localhost:8080/healthz   # {"status":"ok"}
```

## High Level Diagram
<img width="692" height="539" alt="Screenshot 2026-10-12 at 8 32 59 PM" src="https://github.com/user-attachments/assets/b05f6523-5f08-44a1-9ce8-34007d4218cc" />
