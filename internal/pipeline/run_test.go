package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type counter struct {
	mu   sync.Mutex
	seen map[string]int
}

func (c *counter) Observe(stage string, _ time.Duration, _ bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[stage]++
}

func writeFile(t *testing.T, name, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testSpec(csvPath, outPath string) domain.Spec {
	return domain.Spec{
		Sources:         []domain.SourceSpec{{Type: "csv", Location: csvPath}},
		Validation:      []domain.ValidationRule{{Field: "cases", Rule: "min", Value: 0}},
		Transformations: []domain.Transformation{{Field: "country", Op: "lowercase"}},
		Aggregations:    []domain.Aggregation{{Op: "sum", Field: "cases", GroupBy: "country"}},
		Exports:         []domain.ExportSpec{{Type: "json", Path: outPath}},
		Options:         domain.Options{Workers: 2, BufferSize: 4},
	}
}

func TestPlan_Run(t *testing.T) {
	csvPath := writeFile(t, "in.csv", "country,cases\nIndia,500\nINDIA,300\nUSA,-20\nBrazil,150\n")
	outPath := filepath.Join(t.TempDir(), "out.json")

	plan, err := NewPlan(testSpec(csvPath, outPath), Deps{})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}

	obs := &counter{seen: map[string]int{}}
	out, err := plan.Run(t.Context(), "job_1", obs)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if out.Records != 3 || len(out.Errors) != 1 {
		t.Errorf("records = %d, errors = %d; want 3 and 1", out.Records, len(out.Errors))
	}
	if got := out.Summary[0].Values["india"]; got != 800 {
		t.Errorf("sum india = %v, want 800", got)
	}
	if obs.seen["validate"] != 4 || obs.seen["transform"] != 3 {
		t.Errorf("observed %v, want validate 4, transform 3", obs.seen)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Errorf("export file missing: %v", err)
	}
}

func TestNewPlan_BadSpec(t *testing.T) {
	tests := []struct {
		name   string
		change func(*domain.Spec)
	}{
		{"bad rule", func(s *domain.Spec) { s.Validation[0].Rule = "minn" }},
		{"bad op", func(s *domain.Spec) { s.Transformations[0].Op = "reverse" }},
		{"bad aggregation", func(s *domain.Spec) { s.Aggregations[0].Op = "median" }},
		{"db export without db", func(s *domain.Spec) { s.Exports[0] = domain.ExportSpec{Type: "db"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := testSpec("in.csv", "out.json")
			tt.change(&spec)
			if _, err := NewPlan(spec, Deps{}); err == nil {
				t.Error("want error, got nil")
			}
		})
	}
}

func TestPlan_RunMissingFile(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	plan, err := NewPlan(testSpec("does-not-exist.csv", outPath), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Run(t.Context(), "job_1", nil); err == nil {
		t.Error("want error for missing file")
	}
	if _, err := os.Stat(outPath); err == nil {
		t.Error("nothing should be exported when ingest fails")
	}
}

func TestPlan_RunCancelled(t *testing.T) {
	csvPath := writeFile(t, "in.csv", "country,cases\nIndia,500\n")
	plan, err := NewPlan(testSpec(csvPath, filepath.Join(t.TempDir(), "out.json")), Deps{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := plan.Run(ctx, "job_1", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
