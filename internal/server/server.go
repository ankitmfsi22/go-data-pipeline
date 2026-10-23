package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/config"
)

type Server struct {
	http   *http.Server
	log    *slog.Logger
	ready  atomic.Bool
	checks map[string]func(context.Context) error
}

func New(cfg config.Config, log *slog.Logger, register func(mux *http.ServeMux), middleware ...func(http.Handler) http.Handler) *Server {
	s := &Server{log: log, checks: map[string]func(context.Context) error{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	if register != nil {
		register(mux)
	}
	var handler http.Handler = mux
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}

	s.http = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
	return s
}

func (s *Server) Handler() http.Handler { return s.http.Handler }
func (s *Server) AddCheck(name string, check func(context.Context) error) {
	s.checks[name] = check
}
func (s *Server) Start() error {
	s.ready.Store(true)
	s.log.Info("http server listening", "addr", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.ready.Store(false)
	s.log.Info("http server shutting down")
	return s.http.Shutdown(ctx)
}
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status, results := http.StatusOK, map[string]string{}
	for name, check := range s.checks {
		if err := check(ctx); err != nil {
			status, results[name] = http.StatusServiceUnavailable, "error"
			s.log.Error("readiness check failed", "check", name, "err", err)
			continue
		}
		results[name] = "ok"
	}

	word := "ready"
	if status != http.StatusOK {
		word = "not ready"
	}
	writeJSON(w, status, map[string]any{"status": word, "checks": results})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
