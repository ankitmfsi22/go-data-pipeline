package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func TestNewTransformer_BadOps(t *testing.T) {
	if _, err := NewTransformer([]domain.Transformation{{Field: "x", Op: "reverse"}}); err == nil {
		t.Fatal("expected error for unknown op")
	}
	if _, err := NewTransformer([]domain.Transformation{{Op: "trim"}}); err == nil {
		t.Fatal("expected error for missing field")
	}
}

func TestTransformer_Transform(t *testing.T) {
	tr, err := NewTransformer([]domain.Transformation{
		{Field: "country", Op: "lowercase"},
		{Field: "symbol", Op: "uppercase"},
		{Field: "cases", Op: "to_number"},
		{Field: "price", Op: "to_number"},
	})
	if err != nil {
		t.Fatalf("NewTransformer: %v", err)
	}

	in := rec(map[string]any{
		"country": "INDIA ",
		"symbol":  "btc",
		"cases":   "300",
		"price":   json.Number("67000.5"),
	})
	out, err := tr.Transform(in)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}

	if out.Fields["country"] != "india" {
		t.Errorf("country = %q, want india", out.Fields["country"])
	}
	if out.Fields["symbol"] != "BTC" {
		t.Errorf("symbol = %q, want BTC", out.Fields["symbol"])
	}
	if out.Fields["cases"] != 300.0 {
		t.Errorf("cases = %v (%T), want 300 float64", out.Fields["cases"], out.Fields["cases"])
	}
	if out.Fields["price"] != 67000.5 {
		t.Errorf("price = %v, want 67000.5", out.Fields["price"])
	}
	if in.Fields["country"] != "INDIA " {
		t.Errorf("original record was changed: country = %q", in.Fields["country"])
	}
}

func TestTransformer_ToNumberFails(t *testing.T) {
	tr, _ := NewTransformer([]domain.Transformation{{Field: "cases", Op: "to_number"}})

	if _, err := tr.Transform(rec(map[string]any{"cases": "abc"})); err == nil {
		t.Fatal("expected error converting abc")
	}
}

func TestTransformer_MissingFieldIsSkipped(t *testing.T) {
	tr, _ := NewTransformer([]domain.Transformation{{Field: "country", Op: "lowercase"}})

	out, err := tr.Transform(rec(map[string]any{"coin": "bitcoin"}))
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if _, ok := out.Fields["country"]; ok {
		t.Error("country should not be added when it was missing")
	}
}
