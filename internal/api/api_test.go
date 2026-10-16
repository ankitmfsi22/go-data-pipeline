package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/job"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

func newTestAPI(t *testing.T) *httptest.Server {
	t.Helper()
	db, err := store.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := job.NewManager(t.Context(), db, log, job.Config{Workers: 2, BufferSize: 4, Timeout: time.Minute, MaxRunning: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })

	mux := http.NewServeMux()
	New(m, log).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, body string, out any) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
	return resp.StatusCode
}

func csvJob(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "covid.csv")
	data := "country,cases\nIndia,500\nINDIA,300\nUSA,-20\nBrazil,150\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := map[string]any{
		"name":            "covid",
		"sources":         []map[string]any{{"type": "csv", "location": path}},
		"validation":      []map[string]any{{"field": "cases", "rule": "min", "value": 0}},
		"transformations": []map[string]any{{"field": "country", "op": "lowercase"}},
		"aggregations":    []map[string]any{{"op": "sum", "field": "cases", "group_by": "country"}},
		"exports":         []map[string]any{{"type": "db"}},
	}
	b, _ := json.Marshal(spec)
	return string(b)
}

func waitDone(t *testing.T, srv *httptest.Server, id string) domain.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var j domain.Job
		call(t, srv, http.MethodGet, "/api/v1/pipelines/"+id, "", &j)
		if j.Status.Done() {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", id)
	return domain.Job{}
}

func TestAPI_FullFlow(t *testing.T) {
	srv := newTestAPI(t)

	var created domain.Job
	if code := call(t, srv, http.MethodPost, "/api/v1/pipelines", csvJob(t), &created); code != http.StatusAccepted {
		t.Fatalf("POST = %d, want 202", code)
	}
	if created.ID == "" || created.Status != domain.StatusPending {
		t.Fatalf("created = %+v", created)
	}
	id := created.ID

	if j := waitDone(t, srv, id); j.Status != domain.StatusCompleted {
		t.Fatalf("status = %s (%s), want completed", j.Status, j.Error)
	}

	var prog struct {
		Status   domain.Status   `json:"status"`
		Progress domain.Progress `json:"progress"`
	}
	call(t, srv, http.MethodGet, "/api/v1/pipelines/"+id+"/progress", "", &prog)
	if prog.Progress.RecordsRead != 4 || prog.Progress.RecordsFailed != 1 {
		t.Errorf("progress = %+v", prog.Progress)
	}
	var res job.Results
	if code := call(t, srv, http.MethodGet, "/api/v1/pipelines/"+id+"/results?limit=2", "", &res); code != http.StatusOK {
		t.Fatalf("results = %d", code)
	}
	if res.TotalRecords != 3 || len(res.Records) != 2 || res.Summary[0].Values["india"] != 800 {
		t.Errorf("results = %+v", res)
	}
	var errs struct {
		Count  int                  `json:"count"`
		Errors []domain.RecordError `json:"errors"`
	}
	call(t, srv, http.MethodGet, "/api/v1/pipelines/"+id+"/errors", "", &errs)
	if errs.Count != 1 || errs.Errors[0].Stage != "validate" || errs.Errors[0].Record.Line != 4 {
		t.Errorf("errors = %+v", errs)
	}
	var list struct {
		Count int `json:"count"`
	}
	call(t, srv, http.MethodGet, "/api/v1/pipelines", "", &list)
	if list.Count != 1 {
		t.Errorf("list count = %d, want 1", list.Count)
	}
	if code := call(t, srv, http.MethodPatch, "/api/v1/pipelines/"+id+"/cancel", "", nil); code != http.StatusConflict {
		t.Errorf("cancel finished = %d, want 409", code)
	}
	if code := call(t, srv, http.MethodDelete, "/api/v1/pipelines/"+id, "", nil); code != http.StatusNoContent {
		t.Errorf("DELETE = %d, want 204", code)
	}
	if code := call(t, srv, http.MethodGet, "/api/v1/pipelines/"+id, "", nil); code != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want 404", code)
	}
}

func TestAPI_CancelRunningJob(t *testing.T) {
	srv := newTestAPI(t)
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(slow.Close)
	body := `{"name":"slow","sources":[{"type":"http","location":"` + slow.URL + `"}],"exports":[{"type":"db"}]}`

	var created domain.Job
	call(t, srv, http.MethodPost, "/api/v1/pipelines", body, &created)
	if code := call(t, srv, http.MethodGet, "/api/v1/pipelines/"+created.ID+"/results", "", nil); code != http.StatusConflict {
		t.Errorf("results while running = %d, want 409", code)
	}

	if code := call(t, srv, http.MethodPatch, "/api/v1/pipelines/"+created.ID+"/cancel", "", nil); code != http.StatusAccepted {
		t.Fatalf("cancel = %d, want 202", code)
	}
	if j := waitDone(t, srv, created.ID); j.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", j.Status)
	}
}

func TestAPI_BadRequests(t *testing.T) {
	srv := newTestAPI(t)

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"broken json", http.MethodPost, "/api/v1/pipelines", `{"name":`, http.StatusBadRequest},
		{"unknown field", http.MethodPost, "/api/v1/pipelines", `{"valdation":[]}`, http.StatusBadRequest},
		{"invalid spec", http.MethodPost, "/api/v1/pipelines", `{"name":"x"}`, http.StatusBadRequest},
		{"bad rule", http.MethodPost, "/api/v1/pipelines",
			`{"sources":[{"type":"csv","location":"a.csv"}],"validation":[{"field":"a","rule":"minn"}],"exports":[{"type":"db"}]}`,
			http.StatusBadRequest},
		{"unknown id", http.MethodGet, "/api/v1/pipelines/job_nope", "", http.StatusNotFound},
		{"unknown id progress", http.MethodGet, "/api/v1/pipelines/job_nope/progress", "", http.StatusNotFound},
		{"unknown id errors", http.MethodGet, "/api/v1/pipelines/job_nope/errors", "", http.StatusNotFound},
		{"bad limit", http.MethodGet, "/api/v1/pipelines/job_nope/results?limit=abc", "", http.StatusBadRequest},
		{"wrong method", http.MethodPut, "/api/v1/pipelines", "", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := call(t, srv, tt.method, tt.path, tt.body, nil)
			if code != tt.want {
				t.Errorf("%s %s = %d, want %d", tt.method, tt.path, code, tt.want)
			}
		})
	}
}
