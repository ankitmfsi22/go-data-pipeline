// Command server is the entrypoint of the data pipeline service.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ankitmfsi22/go-data-pipeline/internal/api"
	"github.com/ankitmfsi22/go-data-pipeline/internal/config"
	"github.com/ankitmfsi22/go-data-pipeline/internal/job"
	"github.com/ankitmfsi22/go-data-pipeline/internal/logger"
	"github.com/ankitmfsi22/go-data-pipeline/internal/server"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.OpenSQLite(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	jobs, err := job.NewManager(ctx, db, log, job.Config{
		Workers:    cfg.DefaultWorkers,
		BufferSize: cfg.DefaultBufferSize,
		Timeout:    cfg.DefaultJobTimeout,
		MaxRunning: cfg.MaxConcurrentJobs,
	})
	if err != nil {
		return err
	}
	srv := server.New(cfg, log, api.New(jobs, log).Register)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	if err := jobs.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("stop jobs: %w", err)
	}
	log.Info("server stopped cleanly")
	return nil
}
