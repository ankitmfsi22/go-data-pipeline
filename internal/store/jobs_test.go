package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func openTestDB(t *testing.T) *SQLite {
	t.Helper()
	db, err := OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newJob(id string) domain.Job {
	return domain.Job{
		ID:        id,
		Name:      "covid",
		Status:    domain.StatusPending,
		Spec:      domain.Spec{Name: "covid", Sources: []domain.SourceSpec{{Type: "csv", Location: "a.csv"}}},
		CreatedAt: time.Now().UTC(),
	}
}

func TestJobs_CreateGetUpdate(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	if err := db.CreateJob(ctx, newJob("job_1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	j, err := db.GetJob(ctx, "job_1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if j.Status != domain.StatusPending || j.Spec.Sources[0].Location != "a.csv" || j.StartedAt != nil {
		t.Errorf("got %+v", j)
	}

	now := time.Now().UTC()
	j.Status, j.StartedAt, j.FinishedAt = domain.StatusCompleted, &now, &now
	if err := db.UpdateJob(ctx, j); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	j, _ = db.GetJob(ctx, "job_1")
	if j.Status != domain.StatusCompleted || j.FinishedAt == nil {
		t.Errorf("after update got %+v", j)
	}
}

func TestJobs_NotFound(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.GetJob(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetJob err = %v, want ErrNotFound", err)
	}
	if err := db.UpdateJob(t.Context(), newJob("nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("UpdateJob err = %v, want ErrNotFound", err)
	}
	if err := db.DeleteJob(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteJob err = %v, want ErrNotFound", err)
	}
}

func TestJobs_List(t *testing.T) {
	db := openTestDB(t)
	old := newJob("job_old")
	old.CreatedAt = time.Now().UTC().Add(-time.Hour)
	for _, j := range []domain.Job{old, newJob("job_new")} {
		if err := db.CreateJob(t.Context(), j); err != nil {
			t.Fatal(err)
		}
	}

	jobs, err := db.ListJobs(t.Context())
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 2 || jobs[0].ID != "job_new" {
		t.Errorf("got %d jobs, first %q; want 2, newest first", len(jobs), jobs[0].ID)
	}
}

func TestJobs_ErrorsAndDelete(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()
	if err := db.CreateJob(ctx, newJob("job_1")); err != nil {
		t.Fatal(err)
	}

	errs := []domain.RecordError{
		{Stage: "validate", Reason: "cases must be >= 0", Record: domain.Record{Source: "a.csv", Line: 4, Fields: map[string]any{"cases": "-20"}}},
		{Stage: "transform", Reason: "cases is not a number", Record: domain.Record{Source: "a.csv", Line: 2, Fields: map[string]any{"cases": "abc"}}},
	}
	if err := db.SaveErrors(ctx, "job_1", errs); err != nil {
		t.Fatalf("SaveErrors: %v", err)
	}
	rec := []domain.Record{{Source: "a.csv", Line: 3, Fields: map[string]any{"cases": 5}}}
	if err := db.SaveResults(ctx, "job_1", rec, nil); err != nil {
		t.Fatal(err)
	}

	got, err := db.ListErrors(ctx, "job_1")
	if err != nil {
		t.Fatalf("ListErrors: %v", err)
	}
	if len(got) != 2 || got[0].Record.Line != 2 || got[1].Record.Fields["cases"] != "-20" {
		t.Errorf("got %+v", got)
	}

	if err := db.DeleteJob(ctx, "job_1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if got, _ := db.ListErrors(ctx, "job_1"); len(got) != 0 {
		t.Errorf("errors left after delete: %d", len(got))
	}
	if n, _ := db.CountRecords(ctx, "job_1"); n != 0 {
		t.Errorf("records left after delete: %d", n)
	}
}

func TestJobs_FailUnfinished(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	running := newJob("job_running")
	running.Status = domain.StatusRunning
	done := newJob("job_done")
	done.Status = domain.StatusCompleted
	for _, j := range []domain.Job{newJob("job_pending"), running, done} {
		if err := db.CreateJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	n, err := db.FailUnfinished(ctx)
	if err != nil || n != 2 {
		t.Fatalf("FailUnfinished = %d, %v; want 2", n, err)
	}
	if j, _ := db.GetJob(ctx, "job_done"); j.Status != domain.StatusCompleted {
		t.Errorf("finished job changed to %s", j.Status)
	}
	if j, _ := db.GetJob(ctx, "job_running"); j.Status != domain.StatusFailed || j.Error == "" {
		t.Errorf("running job = %s %q, want failed with a reason", j.Status, j.Error)
	}
}
