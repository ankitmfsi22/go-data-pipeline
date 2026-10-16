package store

import (
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func TestResults_SummaryAndRecords(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()

	records := []domain.Record{
		{Source: "a.csv", Line: 3, Fields: map[string]any{"country": "india"}},
		{Source: "a.csv", Line: 2, Fields: map[string]any{"country": "brazil"}},
		{Source: "a.csv", Line: 4, Fields: map[string]any{"country": "india"}},
	}
	summary := []domain.AggResult{
		{Op: "count", Values: map[string]float64{"all": 3}},
		{Op: "sum", Field: "cases", GroupBy: "country", Values: map[string]float64{"india": 800, "brazil": 150}},
	}
	if err := db.SaveResults(ctx, "job_1", records, summary); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetSummary(ctx, "job_1")
	if err != nil {
		t.Fatalf("GetSummary: %v", err)
	}
	if len(got) != 2 || got[0].Op != "count" || got[1].Values["india"] != 800 || got[1].Values["brazil"] != 150 {
		t.Errorf("summary = %+v", got)
	}

	page, err := db.ListRecords(ctx, "job_1", 2, 0)
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if len(page) != 2 || page[0].Line != 2 || page[1].Line != 3 {
		t.Errorf("first page = %+v, want lines 2 and 3", page)
	}
	if page, _ := db.ListRecords(ctx, "job_1", 2, 2); len(page) != 1 || page[0].Line != 4 {
		t.Errorf("second page = %+v, want line 4", page)
	}

	if got, _ := db.GetSummary(ctx, "job_nope"); len(got) != 0 {
		t.Errorf("unknown job summary = %+v, want empty", got)
	}
}
