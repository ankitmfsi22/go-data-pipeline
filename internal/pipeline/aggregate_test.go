package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func TestNewAggregator_BadSpecs(t *testing.T) {
	bad := []domain.Aggregation{
		{Op: "median", Field: "x"},
		{Op: "sum"},
	}
	for _, s := range bad {
		if _, err := NewAggregator([]domain.Aggregation{s}); err == nil {
			t.Errorf("NewAggregator(%+v) expected error", s)
		}
	}
}

func TestAggregator_Run(t *testing.T) {
	agg, err := NewAggregator([]domain.Aggregation{
		{Op: "count"},
		{Op: "sum", Field: "cases", GroupBy: "country"},
		{Op: "avg", Field: "cases"},
		{Op: "max", Field: "cases", GroupBy: "country"},
	})
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}

	in := make(chan domain.Record, 10)
	for _, f := range []map[string]any{
		{"country": "india", "cases": 500.0},
		{"country": "india", "cases": 300.0},
		{"country": "brazil", "cases": 150.0},
		{"coin": "bitcoin"},
	} {
		in <- rec(f)
	}
	close(in)

	out := make(chan domain.Record, 10)
	results, err := agg.Run(t.Context(), in, out)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	passed := 0
	for range out {
		passed++
	}
	if passed != 4 {
		t.Errorf("records passed through = %d, want 4", passed)
	}

	check := func(i int, group string, want float64) {
		t.Helper()
		if got := results[i].Values[group]; got != want {
			t.Errorf("%s(%s)[%s] = %v, want %v", results[i].Op, results[i].Field, group, got, want)
		}
	}
	check(0, "all", 4)
	check(1, "india", 800)
	check(1, "brazil", 150)
	check(2, "all", 950.0/3)
	check(3, "india", 500)
	if _, ok := results[1].Values["bitcoin"]; ok {
		t.Error("record without country should not create a group")
	}
}

func TestAggregator_StopsOnCancel(t *testing.T) {
	agg, _ := NewAggregator([]domain.Aggregation{{Op: "count"}})
	ctx, cancel := context.WithCancel(t.Context())

	in := make(chan domain.Record)
	errCh := make(chan error, 1)
	go func() {
		_, err := agg.Run(ctx, in, make(chan domain.Record))
		errCh <- err
	}()

	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected context error")
		}
	case <-time.After(time.Second):
		t.Fatal("aggregator did not stop after cancel")
	}
}
