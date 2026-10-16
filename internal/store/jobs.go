package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

var ErrNotFound = errors.New("not found")

func (s *SQLite) CreateJob(ctx context.Context, j domain.Job) error {
	spec, err := json.Marshal(j.Spec)
	if err != nil {
		return fmt.Errorf("encode spec: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, name, status, error, spec, created_at, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Name, j.Status, j.Error, string(spec), j.CreatedAt, j.StartedAt, j.FinishedAt)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}
func (s *SQLite) UpdateJob(ctx context.Context, j domain.Job) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ?, error = ?, started_at = ?, finished_at = ? WHERE id = ?`,
		j.Status, j.Error, j.StartedAt, j.FinishedAt, j.ID)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

const jobColumns = `id, name, status, error, spec, created_at, started_at, finished_at`

func (s *SQLite) GetJob(ctx context.Context, id string) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, ErrNotFound
	}
	return j, err
}
func (s *SQLite) ListJobs(ctx context.Context) ([]domain.Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	jobs := []domain.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
func (s *SQLite) FailUnfinished(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ?, error = ?, finished_at = ? WHERE status IN (?, ?)`,
		domain.StatusFailed, "server restarted before the job finished", time.Now().UTC(),
		domain.StatusPending, domain.StatusRunning)
	if err != nil {
		return 0, fmt.Errorf("fail unfinished jobs: %w", err)
	}
	return res.RowsAffected()
}
func (s *SQLite) DeleteJob(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	for _, q := range []string{
		`DELETE FROM job_records WHERE job_id = ?`,
		`DELETE FROM job_summaries WHERE job_id = ?`,
		`DELETE FROM job_errors WHERE job_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return fmt.Errorf("delete job data: %w", err)
		}
	}
	return tx.Commit()
}
func (s *SQLite) SaveErrors(ctx context.Context, jobID string, errs []domain.RecordError) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO job_errors (job_id, stage, source, line, reason, data) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, e := range errs {
		data, err := json.Marshal(e.Record.Fields)
		if err != nil {
			return fmt.Errorf("encode record: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, jobID, e.Stage, e.Record.Source, e.Record.Line, e.Reason, string(data)); err != nil {
			return fmt.Errorf("insert error: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SQLite) ListErrors(ctx context.Context, jobID string) ([]domain.RecordError, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT stage, source, line, reason, data FROM job_errors WHERE job_id = ? ORDER BY source, line`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list errors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []domain.RecordError{}
	for rows.Next() {
		var e domain.RecordError
		var data string
		if err := rows.Scan(&e.Stage, &e.Record.Source, &e.Record.Line, &e.Reason, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(data), &e.Record.Fields); err != nil {
			return nil, fmt.Errorf("decode record: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(sc scanner) (domain.Job, error) {
	var (
		j                 domain.Job
		spec              string
		started, finished sql.NullTime
	)
	if err := sc.Scan(&j.ID, &j.Name, &j.Status, &j.Error, &spec, &j.CreatedAt, &started, &finished); err != nil {
		return domain.Job{}, err
	}
	if err := json.Unmarshal([]byte(spec), &j.Spec); err != nil {
		return domain.Job{}, fmt.Errorf("decode spec: %w", err)
	}
	if started.Valid {
		j.StartedAt = &started.Time
	}
	if finished.Valid {
		j.FinishedAt = &finished.Time
	}
	return j, nil
}
