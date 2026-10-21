package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/metrics"
)

func get(t *testing.T, h http.Handler, path string, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRequestID(t *testing.T) {
	var seen string
	h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))

	rec := get(t, h, "/", nil)
	if seen == "" || rec.Header().Get("X-Request-ID") != seen {
		t.Errorf("new id: context %q, header %q", seen, rec.Header().Get("X-Request-ID"))
	}

	rec = get(t, h, "/", map[string]string{"X-Request-ID": "abc-123"})
	if seen != "abc-123" || rec.Header().Get("X-Request-ID") != "abc-123" {
		t.Errorf("caller's id not kept: %q", seen)
	}
}

func TestRecover(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	h := Recover(log)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := get(t, h, "/", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Errorf("panic was not logged: %s", logs.String())
	}
}

func TestLogging(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	h := RequestID(Logging(log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})))

	get(t, h, "/hello", map[string]string{"X-Request-ID": "req-1"})
	line := logs.String()
	for _, want := range []string{"path=/hello", "status=418", "request_id=req-1"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line missing %q: %s", want, line)
		}
	}
}

func TestMetricsMiddleware(t *testing.T) {
	m := metrics.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/pipelines/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	h := Metrics(m)(mux)

	get(t, h, "/api/v1/pipelines/job_1", nil)
	get(t, h, "/api/v1/pipelines/job_2", nil)
	get(t, h, "/nope", nil)

	if got := m.HTTPRequests.Value("GET /api/v1/pipelines/{id}", "404"); got != 2 {
		t.Errorf("route count = %d, want 2", got)
	}
	if got := m.HTTPRequests.Value("unmatched", "404"); got != 1 {
		t.Errorf("unmatched count = %d, want 1", got)
	}
}
