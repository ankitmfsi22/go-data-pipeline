package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func rec(fields map[string]any) domain.Record {
	return domain.Record{Source: "test.csv", Line: 2, Fields: fields}
}

func TestNewValidator_BadRules(t *testing.T) {
	tests := []struct {
		name string
		rule domain.ValidationRule
	}{
		{"missing field", domain.ValidationRule{Rule: "required"}},
		{"unknown rule", domain.ValidationRule{Field: "x", Rule: "email"}},
		{"min without number", domain.ValidationRule{Field: "x", Rule: "min", Value: "ten"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewValidator([]domain.ValidationRule{tt.rule}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestValidator_Validate(t *testing.T) {
	v, err := NewValidator([]domain.ValidationRule{
		{Field: "country", Rule: "required"},
		{Field: "cases", Rule: "min", Value: 0},
		{Field: "cases", Rule: "max", Value: 1000},
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	tests := []struct {
		name    string
		fields  map[string]any
		wantErr bool
	}{
		{"valid string number", map[string]any{"country": "India", "cases": "500"}, false},
		{"valid json number", map[string]any{"country": "India", "cases": json.Number("10")}, false},
		{"negative", map[string]any{"country": "USA", "cases": "-20"}, true},
		{"not a number", map[string]any{"country": "USA", "cases": "abc"}, true},
		{"too big", map[string]any{"country": "USA", "cases": "5000"}, true},
		{"blank country", map[string]any{"country": "  ", "cases": "5"}, true},
		{"missing country", map[string]any{"cases": "5"}, true},
		{"missing cases is skipped", map[string]any{"country": "India"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.Validate(rec(tt.fields))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
