// Package server owns the HTTP server lifecycle: routing, timeouts,
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/ankitmfsi22/go-data-pipeline/internal/config"
)

// Server wraps http.Server
type Server struct {
	http  *http.Server
	log   *slog.Logger
	ready atomic.Bool
}

// New builds a Server. Extra routes can be added through register
func New(cfg config.Config, log *slog.Logger, register func(mux *http.ServeMux)) *Server {
	s := &Server{log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	if register != nil {
		register(mux)
	}

	s.http = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
	return s
}

// Handler exposes the root handler
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Start begins serving and blocks until the server stops.
func (s *Server) Start() error {
	s.ready.Store(true)
	s.log.Info("http server listening", "addr", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown marks the server not-ready,
func (s *Server) Shutdown(ctx context.Context) error {
	s.ready.Store(false)
	s.log.Info("http server shutting down")
	return s.http.Shutdown(ctx)
}
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
