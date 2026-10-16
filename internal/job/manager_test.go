package job

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

func newTestManager(t *testing.T, maxRunning int) (*Manager, *store.SQLite) {
	t.Helper()
	db, err := store.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := NewManager(t.Context(), db, log, Config{Workers: 2, BufferSize: 4, Timeout: time.Minute, MaxRunning: maxRunning})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	return m, db
}

func csvSpec(t *testing.T) domain.Spec {
	t.Helper()
	path := filepath.Join(t.TempDir(), "covid.csv")
	data := "country,cases\nIndia,500\nINDIA,300\nUSA,-20\nBrazil,150\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return domain.Spec{
		Name:            "covid",
		Sources:         []domain.SourceSpec{{Type: "csv", Location: path}},
		Validation:      []domain.ValidationRule{{Field: "cases", Rule: "min", Value: 0}},
		Transformations: []domain.Transformation{{Field: "country", Op: "lowercase"}},
		Aggregations:    []domain.Aggregation{{Op: "sum", Field: "cases", GroupBy: "country"}},
		Exports:         []domain.ExportSpec{{Type: "db"}},
	}
}
func slowSpec(t *testing.T) domain.Spec {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return domain.Spec{
		Name:    "slow",
		Sources: []domain.SourceSpec{{Type: "http", Location: srv.URL}},
		Exports: []domain.ExportSpec{{Type: "db"}},
	}
}
func waitFor(t *testing.T, m *Manager, id string) domain.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := m.Get(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status.Done() {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish in time", id)
	return domain.Job{}
}

func waitForStatus(t *testing.T, m *Manager, id string, want domain.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j, _ := m.Get(t.Context(), id); j.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s", id, want)
}

func TestManager_RunsJob(t *testing.T) {
	m, db := newTestManager(t, 2)

	j, err := m.Start(t.Context(), csvSpec(t))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if j.Status != domain.StatusPending || j.Spec.Options.Workers != 2 {
		t.Errorf("new job = %s, workers %d; want pending with defaults applied", j.Status, j.Spec.Options.Workers)
	}

	j = waitFor(t, m, j.ID)
	if j.Status != domain.StatusCompleted || j.StartedAt == nil || j.FinishedAt == nil {
		t.Fatalf("finished job = %+v", j)
	}

	p, ok := m.Progress(j.ID)
	if !ok || p.RecordsRead != 4 || p.RecordsPassed != 3 || p.RecordsFailed != 1 {
		t.Errorf("progress = %+v", p)
	}

	if n, _ := db.CountRecords(t.Context(), j.ID); n != 3 {
		t.Errorf("saved records = %d, want 3", n)
	}
	if v, _ := db.SummaryValue(t.Context(), j.ID, "sum", "cases", "india"); v != 800 {
		t.Errorf("sum india = %v, want 800", v)
	}
	if errs, _ := db.ListErrors(t.Context(), j.ID); len(errs) != 1 {
		t.Errorf("saved errors = %d, want 1", len(errs))
	}
}

func TestManager_InvalidSpec(t *testing.T) {
	m, _ := newTestManager(t, 1)

	spec := csvSpec(t)
	spec.Validation[0].Rule = "minn"
	if _, err := m.Start(t.Context(), spec); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("err = %v, want ErrInvalidSpec", err)
	}

	if _, err := m.Start(t.Context(), domain.Spec{}); !errors.Is(err, ErrInvalidSpec) {
		t.Errorf("empty spec err = %v, want ErrInvalidSpec", err)
	}
}

func TestManager_FailedJob(t *testing.T) {
	m, _ := newTestManager(t, 1)

	spec := csvSpec(t)
	spec.Sources[0].Location = "does-not-exist.csv"
	j, err := m.Start(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if j = waitFor(t, m, j.ID); j.Status != domain.StatusFailed || j.Error == "" {
		t.Errorf("got %s %q, want failed with a reason", j.Status, j.Error)
	}
}

func TestManager_Cancel(t *testing.T) {
	m, _ := newTestManager(t, 1)

	j, err := m.Start(t.Context(), slowSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, m, j.ID, domain.StatusRunning)

	if err := m.Cancel(t.Context(), j.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if j = waitFor(t, m, j.ID); j.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", j.Status)
	}
	if err := m.Cancel(t.Context(), j.ID); !errors.Is(err, ErrFinished) {
		t.Errorf("second cancel err = %v, want ErrFinished", err)
	}
	if err := m.Cancel(t.Context(), "job_nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id err = %v, want ErrNotFound", err)
	}
}

func TestManager_Timeout(t *testing.T) {
	m, _ := newTestManager(t, 1)

	spec := slowSpec(t)
	spec.Options.Timeout = domain.Duration(50 * time.Millisecond)
	j, err := m.Start(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if j = waitFor(t, m, j.ID); j.Status != domain.StatusFailed || j.Error != "timed out after 50ms" {
		t.Errorf("got %s %q, want failed, timed out", j.Status, j.Error)
	}
}

func TestManager_MaxRunning(t *testing.T) {
	m, _ := newTestManager(t, 1) // only one job at a time

	first, err := m.Start(t.Context(), slowSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, m, first.ID, domain.StatusRunning)

	second, err := m.Start(t.Context(), csvSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if j, _ := m.Get(t.Context(), second.ID); j.Status != domain.StatusPending {
		t.Errorf("second job = %s, want pending while the first one runs", j.Status)
	}

	// free the slot: the waiting job should now run to the end
	if err := m.Cancel(t.Context(), first.ID); err != nil {
		t.Fatal(err)
	}
	if j := waitFor(t, m, second.ID); j.Status != domain.StatusCompleted {
		t.Errorf("second job = %s, want completed", j.Status)
	}
}

func TestManager_Delete(t *testing.T) {
	m, db := newTestManager(t, 1)

	slow, err := m.Start(t.Context(), slowSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Delete(t.Context(), slow.ID); !errors.Is(err, ErrNotFinished) {
		t.Errorf("delete running job err = %v, want ErrNotFinished", err)
	}
	if err := m.Cancel(t.Context(), slow.ID); err != nil {
		t.Fatal(err)
	}

	j, err := m.Start(t.Context(), csvSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, m, j.ID)

	if err := m.Delete(t.Context(), j.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := m.Get(t.Context(), j.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete err = %v, want ErrNotFound", err)
	}
	if _, ok := m.Progress(j.ID); ok {
		t.Error("progress still there after delete")
	}
	if n, _ := db.CountRecords(t.Context(), j.ID); n != 0 {
		t.Errorf("records left = %d", n)
	}
}
