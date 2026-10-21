package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/job"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func run() error {
	specPath := "examples/covid-job.json"
	if len(os.Args) > 1 {
		specPath = os.Args[1]
	}

	data, err := os.ReadFile(specPath) //nolint:gosec // path comes from the person running the demo
	if err != nil {
		return err
	}
	var spec domain.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return fmt.Errorf("read %s: %w", specPath, err)
	}

	ctx := context.Background()
	db, err := store.OpenSQLite(ctx, "output/pipeline.db")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	m, err := job.NewManager(ctx, db, log, job.Config{
		Workers: 4, BufferSize: 100, Timeout: 5 * time.Minute, MaxRunning: 2,
	})
	if err != nil {
		return err
	}

	j, err := m.Start(ctx, spec)
	if err != nil {
		return err
	}
	fmt.Printf("started %s (%s)\n", j.ID, j.Status)
	for !j.Status.Done() {
		time.Sleep(50 * time.Millisecond)
		if j, err = m.Get(ctx, j.ID); err != nil {
			return err
		}
	}

	p, _ := m.Progress(j.ID)
	fmt.Printf("finished: %s %s\n", j.Status, j.Error)
	fmt.Printf("read %d, passed %d, failed %d, %.0f records/sec, took %v\n",
		p.RecordsRead, p.RecordsPassed, p.RecordsFailed, p.RecordsPerSec, p.Elapsed.Std())
	fmt.Printf("latency per record (ms): %v\n", p.StageLatencyMs)

	rejected, err := db.ListErrors(ctx, j.ID)
	if err != nil {
		return err
	}
	fmt.Println("\n== rejected ==")
	for _, e := range rejected {
		fmt.Printf("%s line %d (%s): %s\n", e.Record.Source, e.Record.Line, e.Stage, e.Reason)
	}

	fmt.Println("\nsaved in output/pipeline.db with job id", j.ID)
	return nil
}
