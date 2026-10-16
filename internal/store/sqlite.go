package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS job_records (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id  TEXT    NOT NULL,
	source  TEXT    NOT NULL,
	line    INTEGER NOT NULL,
	data    TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_records_job ON job_records(job_id);

CREATE TABLE IF NOT EXISTS job_summaries (
	id        INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id    TEXT NOT NULL,
	op        TEXT NOT NULL,
	field     TEXT NOT NULL,
	group_by  TEXT NOT NULL,
	group_key TEXT NOT NULL,
	value     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_summaries_job ON job_summaries(job_id);

CREATE TABLE IF NOT EXISTS jobs (
	id          TEXT PRIMARY KEY,
	name        TEXT     NOT NULL,
	status      TEXT     NOT NULL,
	error       TEXT     NOT NULL DEFAULT '',
	spec        TEXT     NOT NULL,
	created_at  DATETIME NOT NULL,
	started_at  DATETIME,
	finished_at DATETIME
);

CREATE TABLE IF NOT EXISTS job_errors (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id  TEXT    NOT NULL,
	stage   TEXT    NOT NULL,
	source  TEXT    NOT NULL,
	line    INTEGER NOT NULL,
	reason  TEXT    NOT NULL,
	data    TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_errors_job ON job_errors(job_id);
`

type SQLite struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, path string) (*SQLite, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create db folder: %w", err)
	}

	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) SaveResults(ctx context.Context, jobID string, records []domain.Record, summary []domain.AggResult) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	recStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO job_records (job_id, source, line, data) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare records: %w", err)
	}
	defer func() { _ = recStmt.Close() }()

	for _, r := range records {
		data, err := json.Marshal(r.Fields)
		if err != nil {
			return fmt.Errorf("encode record %s:%d: %w", r.Source, r.Line, err)
		}
		if _, err := recStmt.ExecContext(ctx, jobID, r.Source, r.Line, string(data)); err != nil {
			return fmt.Errorf("insert record: %w", err)
		}
	}

	sumStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO job_summaries (job_id, op, field, group_by, group_key, value) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare summary: %w", err)
	}
	defer func() { _ = sumStmt.Close() }()

	for _, a := range summary {
		for key, value := range a.Values {
			if _, err := sumStmt.ExecContext(ctx, jobID, a.Op, a.Field, a.GroupBy, key, value); err != nil {
				return fmt.Errorf("insert summary: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *SQLite) CountRecords(ctx context.Context, jobID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_records WHERE job_id = ?`, jobID).Scan(&n)
	return n, err
}

func (s *SQLite) SummaryValue(ctx context.Context, jobID, op, field, groupKey string) (float64, error) {
	var v float64
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM job_summaries WHERE job_id = ? AND op = ? AND field = ? AND group_key = ?`,
		jobID, op, field, groupKey).Scan(&v)
	return v, err
}
