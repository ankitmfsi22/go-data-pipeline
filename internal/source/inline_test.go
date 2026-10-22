package source

import (
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func TestInline_Read(t *testing.T) {
	rows := []map[string]any{
		{"country": "India", "cases": 500.0},
		{"country": "USA", "cases": 20.0},
	}
	src, err := New(domain.SourceSpec{Type: domain.SourceInline, Data: rows}, nil)
	if err != nil {
		t.Fatal(err)
	}

	out := make(chan domain.Record, 10)
	if err := src.Read(t.Context(), out); err != nil {
		t.Fatalf("Read: %v", err)
	}
	close(out)

	var got []domain.Record
	for r := range out {
		got = append(got, r)
	}
	if len(got) != 2 || got[0].Source != "inline" || got[1].Line != 2 || got[1].Fields["country"] != "USA" {
		t.Errorf("got %+v", got)
	}
	got[0].Fields["country"] = "changed"
	if rows[0]["country"] != "India" {
		t.Error("record shares its map with the spec")
	}
}
