package export

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func sampleResult() Result {
	return Result{
		JobID: "job_1",
		Records: []domain.Record{
			{Source: "covid.csv", Line: 2, Fields: map[string]any{"country": "india", "cases": 500.0}},
			{Source: "prices.json", Line: 1, Fields: map[string]any{"coin": "bitcoin"}},
		},
		Summary: []domain.AggResult{
			{Op: "sum", Field: "cases", GroupBy: "country", Values: map[string]float64{"india": 500}},
		},
	}
}

func TestJSONFile_Export(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out", "report.json")

	if err := (JSONFile{Path: path}).Export(t.Context(), sampleResult()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var got Result
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("file is not valid JSON: %v", err)
	}
	if got.JobID != "job_1" || len(got.Records) != 2 || got.Summary[0].Values["india"] != 500 {
		t.Errorf("unexpected content: %+v", got)
	}
}

func TestCSVFile_Export(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.csv")

	if err := (CSVFile{Path: path}).Export(t.Context(), sampleResult()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}

	wantHeader := []string{"source", "line", "cases", "coin", "country"}
	if len(rows) != 3 || len(rows[0]) != len(wantHeader) {
		t.Fatalf("rows = %v", rows)
	}
	for i, h := range wantHeader {
		if rows[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, rows[0][i], h)
		}
	}
	if rows[2][2] != "" || rows[2][3] != "bitcoin" || rows[2][4] != "" {
		t.Errorf("bitcoin row = %v", rows[2])
	}
}

type fakeSaver struct {
	jobID string
	count int
}

func (f *fakeSaver) SaveResults(_ context.Context, jobID string, records []domain.Record, _ []domain.AggResult) error {
	f.jobID, f.count = jobID, len(records)
	return nil
}

func TestDB_Export(t *testing.T) {
	saver := &fakeSaver{}
	if err := (DB{Store: saver}).Export(t.Context(), sampleResult()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if saver.jobID != "job_1" || saver.count != 2 {
		t.Errorf("saver got job=%q count=%d", saver.jobID, saver.count)
	}
}

func TestNew(t *testing.T) {
	if _, err := New(domain.ExportSpec{Type: "xml"}, nil); err == nil {
		t.Error("expected error for unknown type")
	}
	if _, err := New(domain.ExportSpec{Type: "db"}, nil); err == nil {
		t.Error("expected error for db export without a store")
	}
	if e, err := New(domain.ExportSpec{Type: "json", Path: "a.json"}, nil); err != nil || e == nil {
		t.Errorf("json export: %v", err)
	}
}

func TestCollect(t *testing.T) {
	in := make(chan domain.Record, 3)
	in <- domain.Record{Line: 1}
	in <- domain.Record{Line: 2}
	close(in)

	recs, err := Collect(t.Context(), in)
	if err != nil || len(recs) != 2 {
		t.Errorf("Collect = %d records, %v; want 2", len(recs), err)
	}
}
