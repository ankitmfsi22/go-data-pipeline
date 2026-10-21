package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/api"
	"github.com/ankitmfsi22/go-data-pipeline/internal/config"
	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/job"
	"github.com/ankitmfsi22/go-data-pipeline/internal/metrics"
	"github.com/ankitmfsi22/go-data-pipeline/internal/server"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

type app struct {
	t   *testing.T
	url string
}

func newApp(t *testing.T, maxRunning int) *app {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, err := store.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "pipeline.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	met := metrics.New()
	jobs, err := job.NewManager(t.Context(), db, log, job.Config{
		Workers: 4, BufferSize: 50, Timeout: time.Minute, MaxRunning: maxRunning, Metrics: met,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background()) })

	routes := func(mux *http.ServeMux) {
		api.New(jobs, log).Register(mux)
		mux.Handle("GET /metrics", met.Handler())
	}
	srv := server.New(cfg, log, routes, api.RequestID, api.Logging(log), api.Metrics(met), api.Recover(log))

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &app{t: t, url: ts.URL}
}

func (a *app) do(method, path, body string) (int, http.Header, []byte) {
	a.t.Helper()
	req, err := http.NewRequestWithContext(a.t.Context(), method, a.url+path, strings.NewReader(body))
	if err != nil {
		a.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		a.t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, data
}

func (a *app) getJSON(path string, wantStatus int, out any) {
	a.t.Helper()
	code, _, body := a.do(http.MethodGet, path, "")
	if code != wantStatus {
		a.t.Fatalf("GET %s = %d, want %d: %s", path, code, wantStatus, body)
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			a.t.Fatalf("GET %s: decode: %v", path, err)
		}
	}
}

func (a *app) start(spec map[string]any) string {
	a.t.Helper()
	b, _ := json.Marshal(spec)
	code, _, body := a.do(http.MethodPost, "/api/v1/pipelines", string(b))
	if code != http.StatusAccepted {
		a.t.Fatalf("POST = %d: %s", code, body)
	}
	var j domain.Job
	if err := json.Unmarshal(body, &j); err != nil {
		a.t.Fatal(err)
	}
	return j.ID
}

func (a *app) wait(id string) domain.Job {
	a.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var j domain.Job
		a.getJSON("/api/v1/pipelines/"+id, http.StatusOK, &j)
		if j.Status.Done() {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.t.Fatalf("job %s did not finish in time", id)
	return domain.Job{}
}

func (a *app) results(id string) job.Results {
	a.t.Helper()
	var res job.Results
	a.getJSON("/api/v1/pipelines/"+id+"/results?limit=1000", http.StatusOK, &res)
	return res
}

func slowSource(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestIntegration_SampleDataJob(t *testing.T) {
	a := newApp(t, 2)
	out := t.TempDir()

	id := a.start(map[string]any{
		"name": "covid-and-prices",
		"sources": []map[string]any{
			{"type": "csv", "location": "../internal/source/testdata/covid.csv"},
			{"type": "json", "location": "../internal/source/testdata/prices.json"},
		},
		"validation": []map[string]any{
			{"field": "cases", "rule": "min", "value": 0},
			{"field": "price_usd", "rule": "min", "value": 0},
		},
		"transformations": []map[string]any{
			{"field": "country", "op": "lowercase"},
			{"field": "cases", "op": "to_number"},
		},
		"aggregations": []map[string]any{
			{"op": "count"},
			{"op": "sum", "field": "cases", "group_by": "country"},
			{"op": "avg", "field": "price_usd"},
		},
		"exports": []map[string]any{
			{"type": "db"},
			{"type": "json", "path": filepath.Join(out, "report.json")},
			{"type": "csv", "path": filepath.Join(out, "report.csv")},
		},
	})

	if j := a.wait(id); j.Status != domain.StatusCompleted {
		t.Fatalf("status = %s (%s)", j.Status, j.Error)
	}
	res := a.results(id)
	if res.TotalRecords != 5 {
		t.Errorf("total records = %d, want 5", res.TotalRecords)
	}
	want := map[string]float64{"count/all": 5, "sum/india": 800, "sum/brazil": 150, "avg/all": 35250.25}
	for _, agg := range res.Summary {
		for key, v := range agg.Values {
			if w, ok := want[agg.Op+"/"+key]; ok && v != w {
				t.Errorf("%s %s = %v, want %v", agg.Op, key, v, w)
			}
		}
	}
	var errs struct {
		Count int `json:"count"`
	}
	a.getJSON("/api/v1/pipelines/"+id+"/errors", http.StatusOK, &errs)
	if errs.Count != 3 {
		t.Errorf("errors = %d, want 3", errs.Count)
	}
	var prog struct {
		Progress domain.Progress `json:"progress"`
	}
	a.getJSON("/api/v1/pipelines/"+id+"/progress", http.StatusOK, &prog)
	if prog.Progress.RecordsRead != 8 || prog.Progress.RecordsPassed != 5 || prog.Progress.RecordsPerSec <= 0 {
		t.Errorf("progress = %+v", prog.Progress)
	}

	data, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatalf("json report: %v", err)
	}
	var report struct {
		Records []domain.Record `json:"records"`
	}
	if err := json.Unmarshal(data, &report); err != nil || len(report.Records) != 5 {
		t.Errorf("json report has %d records (err %v), want 5", len(report.Records), err)
	}
	if _, err := os.Stat(filepath.Join(out, "report.csv")); err != nil {
		t.Errorf("csv report: %v", err)
	}
	_, _, body := a.do(http.MethodGet, "/metrics", "")
	for _, line := range []string{
		`pipeline_jobs_finished_total{status="completed"} 1`,
		`pipeline_records_total{stage="validate"} 8`,
		`pipeline_records_failed_total{stage="validate"} 3`,
		`http_requests_total{route="POST /api/v1/pipelines",status="202"} 1`,
	} {
		if !strings.Contains(string(body), line) {
			t.Errorf("/metrics is missing %q", line)
		}
	}
}

func TestIntegration_HTTPSource(t *testing.T) {
	a := newApp(t, 1)

	weather := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"city":"Delhi","temp":31.5},{"city":"Pune","temp":27},{"city":"Delhi","temp":33.5}]`)
	}))
	t.Cleanup(weather.Close)

	id := a.start(map[string]any{
		"sources":      []map[string]any{{"type": "http", "location": weather.URL}},
		"aggregations": []map[string]any{{"op": "avg", "field": "temp", "group_by": "city"}},
		"exports":      []map[string]any{{"type": "db"}},
	})
	if j := a.wait(id); j.Status != domain.StatusCompleted {
		t.Fatalf("status = %s (%s)", j.Status, j.Error)
	}
	if got := a.results(id).Summary[0].Values["Delhi"]; got != 32.5 {
		t.Errorf("avg Delhi = %v, want 32.5", got)
	}
}

func TestIntegration_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("large file test skipped in -short mode")
	}
	a := newApp(t, 1)
	const rows = 20000
	countries := []string{"India", "USA", "Brazil", "Japan"}
	sums := map[string]float64{}
	var b strings.Builder
	b.WriteString("country,cases\n")
	for i := range rows {
		c := countries[i%len(countries)]
		cases := i % 100
		if i%10 == 0 {
			cases = -1
		} else {
			sums[strings.ToLower(c)] += float64(cases)
		}
		fmt.Fprintf(&b, "%s,%d\n", c, cases)
	}
	path := filepath.Join(t.TempDir(), "big.csv")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	id := a.start(map[string]any{
		"sources":         []map[string]any{{"type": "csv", "location": path}},
		"validation":      []map[string]any{{"field": "cases", "rule": "min", "value": 0}},
		"transformations": []map[string]any{{"field": "country", "op": "lowercase"}},
		"aggregations":    []map[string]any{{"op": "sum", "field": "cases", "group_by": "country"}},
		"exports":         []map[string]any{{"type": "db"}},
	})
	if j := a.wait(id); j.Status != domain.StatusCompleted {
		t.Fatalf("status = %s (%s)", j.Status, j.Error)
	}

	res := a.results(id)
	if res.TotalRecords != rows*9/10 {
		t.Errorf("saved records = %d, want %d", res.TotalRecords, rows*9/10)
	}
	for country, want := range sums {
		if got := res.Summary[0].Values[country]; got != want {
			t.Errorf("sum %s = %v, want %v", country, got, want)
		}
	}

	var prog struct {
		Progress domain.Progress `json:"progress"`
	}
	a.getJSON("/api/v1/pipelines/"+id+"/progress", http.StatusOK, &prog)
	t.Logf("%d records in %v, %.0f records/sec", prog.Progress.RecordsRead, prog.Progress.Elapsed.Std(), prog.Progress.RecordsPerSec)
	if prog.Progress.RecordsFailed != rows/10 {
		t.Errorf("failed = %d, want %d", prog.Progress.RecordsFailed, rows/10)
	}
}

func TestIntegration_ConcurrentJobs(t *testing.T) {
	a := newApp(t, 2)

	path := filepath.Join(t.TempDir(), "small.csv")
	if err := os.WriteFile(path, []byte("n\n1\n2\n3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{
		"sources":      []map[string]any{{"type": "csv", "location": path}},
		"aggregations": []map[string]any{{"op": "sum", "field": "n"}},
		"exports":      []map[string]any{{"type": "db"}},
	}

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids []string
	)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := a.start(spec)
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		}()
	}
	wg.Wait()

	for _, id := range ids {
		if j := a.wait(id); j.Status != domain.StatusCompleted {
			t.Errorf("job %s = %s (%s)", id, j.Status, j.Error)
		}
		if got := a.results(id).Summary[0].Values["all"]; got != 6 {
			t.Errorf("job %s sum = %v, want 6", id, got)
		}
	}

	var list struct {
		Count int `json:"count"`
	}
	a.getJSON("/api/v1/pipelines", http.StatusOK, &list)
	if list.Count != 6 {
		t.Errorf("list count = %d, want 6", list.Count)
	}
}

func TestIntegration_CancelAndTimeout(t *testing.T) {
	a := newApp(t, 2)
	id := a.start(map[string]any{
		"sources": []map[string]any{{"type": "http", "location": slowSource(t)}},
		"exports": []map[string]any{{"type": "db"}},
	})
	if code, _, body := a.do(http.MethodPatch, "/api/v1/pipelines/"+id+"/cancel", ""); code != http.StatusAccepted {
		t.Fatalf("cancel = %d: %s", code, body)
	}
	if j := a.wait(id); j.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", j.Status)
	}
	id = a.start(map[string]any{
		"sources": []map[string]any{{"type": "http", "location": slowSource(t)}},
		"exports": []map[string]any{{"type": "db"}},
		"options": map[string]any{"timeout": "100ms"},
	})
	if j := a.wait(id); j.Status != domain.StatusFailed || !strings.Contains(j.Error, "timed out") {
		t.Errorf("status = %s %q, want failed, timed out", j.Status, j.Error)
	}

	if code, _, _ := a.do(http.MethodDelete, "/api/v1/pipelines/"+id, ""); code != http.StatusNoContent {
		t.Errorf("delete = %d, want 204", code)
	}
	a.getJSON("/api/v1/pipelines/"+id, http.StatusNotFound, nil)
}

func TestIntegration_RequestID(t *testing.T) {
	a := newApp(t, 1)
	_, header, _ := a.do(http.MethodGet, "/healthz", "")
	if header.Get("X-Request-ID") == "" {
		t.Error("response has no X-Request-ID header")
	}
}
