package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/config"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
}

func doRequest(t *testing.T, s *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t)
	if rec := doRequest(t, s, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	s := newTestServer(t)
	if rec := doRequest(t, s, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz before start = %d, want 503", rec.Code)
	}
	s.ready.Store(true)
	if rec := doRequest(t, s, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("GET /readyz when ready = %d, want 200", rec.Code)
	}
}

func TestUnknownMethodRejected(t *testing.T) {
	s := newTestServer(t)
	if rec := doRequest(t, s, http.MethodPost, "/healthz"); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz = %d, want 405", rec.Code)
	}
}
func TestReadyz_Checks(t *testing.T) {
	s := newTestServer(t)
	s.ready.Store(true)

	dbUp := true
	s.AddCheck("database", func(context.Context) error {
		if !dbUp {
			return errors.New("connection refused")
		}
		return nil
	})

	get := func() int {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
		return rec.Code
	}

	if code := get(); code != http.StatusOK {
		t.Errorf("database up: /readyz = %d, want 200", code)
	}
	dbUp = false
	if code := get(); code != http.StatusServiceUnavailable {
		t.Errorf("database down: /readyz = %d, want 503", code)
	}
}
