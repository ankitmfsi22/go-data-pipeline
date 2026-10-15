package store

import (
	"path/filepath"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func TestSQLite_SaveResults(t *testing.T) {
	db, err := OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "data", "test.db")) // "data" folder does not exist yet
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer func() { _ = db.Close() }()

	records := []domain.Record{
		{Source: "covid.csv", Line: 2, Fields: map[string]any{"country": "india", "cases": 500.0}},
		{Source: "covid.csv", Line: 3, Fields: map[string]any{"country": "india", "cases": 300.0}},
	}
	summary := []domain.AggResult{
		{Op: "sum", Field: "cases", GroupBy: "country", Values: map[string]float64{"india": 800}},
	}

	if err := db.SaveResults(t.Context(), "job_1", records, summary); err != nil {
		t.Fatalf("SaveResults: %v", err)
	}

	n, err := db.CountRecords(t.Context(), "job_1")
	if err != nil || n != 2 {
		t.Errorf("CountRecords = %d, %v; want 2", n, err)
	}

	v, err := db.SummaryValue(t.Context(), "job_1", "sum", "cases", "india")
	if err != nil || v != 800 {
		t.Errorf("SummaryValue = %v, %v; want 800", v, err)
	}
	if n, _ := db.CountRecords(t.Context(), "job_2"); n != 0 {
		t.Errorf("job_2 records = %d, want 0", n)
	}
}

func TestOpenSQLite_ReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := OpenSQLite(t.Context(), path)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	rec := []domain.Record{{Source: "a.csv", Line: 2, Fields: map[string]any{"x": 1}}}
	if err := db.SaveResults(t.Context(), "job_1", rec, nil); err != nil {
		t.Fatalf("SaveResults: %v", err)
	}
	_ = db.Close()
	db, err = OpenSQLite(t.Context(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	if n, _ := db.CountRecords(t.Context(), "job_1"); n != 1 {
		t.Errorf("records after reopen = %d, want 1", n)
	}
}
