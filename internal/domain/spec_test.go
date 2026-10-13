package domain

import (
	"encoding/json"
	"testing"
	"time"
)

const sampleSpec = `{
  "name": "covid-report",
  "sources": [
    { "type": "csv",  "location": "testdata/covid.csv" },
    { "type": "http", "location": "https://example.com/data", "timeout": "10s" }
  ],
  "validation":      [{ "field": "cases", "rule": "min", "value": 0 }],
  "transformations": [{ "field": "country", "op": "lowercase" }],
  "aggregations":    [{ "op": "sum", "field": "cases", "group_by": "country" }],
  "exports":         [{ "type": "json", "path": "output/report.json" }],
  "options":         { "workers": 5, "buffer_size": 100, "timeout": "5m" }
}`

func TestSpec_DecodeAndValidate(t *testing.T) {
	var s Spec
	if err := json.Unmarshal([]byte(sampleSpec), &s); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	if s.Name != "covid-report" {
		t.Errorf("Name = %q, want covid-report", s.Name)
	}
	if len(s.Sources) != 2 {
		t.Fatalf("len(Sources) = %d, want 2", len(s.Sources))
	}
	if got := s.Sources[1].Timeout.Std(); got != 10*time.Second {
		t.Errorf("Sources[1].Timeout = %v, want 10s", got)
	}
	if got := s.Options.Timeout.Std(); got != 5*time.Minute {
		t.Errorf("Options.Timeout = %v, want 5m", got)
	}
}
func TestSpec_ValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
	}{
		{"no sources", Spec{Exports: []ExportSpec{{Type: "db"}}}},
		{"unknown source type", Spec{
			Sources: []SourceSpec{{Type: "xml", Location: "a.xml"}},
			Exports: []ExportSpec{{Type: "db"}},
		}},
		{"missing location", Spec{
			Sources: []SourceSpec{{Type: "csv"}},
			Exports: []ExportSpec{{Type: "db"}},
		}},
		{"no exports", Spec{Sources: []SourceSpec{{Type: "csv", Location: "a.csv"}}}},
		{"file export without path", Spec{
			Sources: []SourceSpec{{Type: "csv", Location: "a.csv"}},
			Exports: []ExportSpec{{Type: "json"}},
		}},
		{"negative workers", Spec{
			Sources: []SourceSpec{{Type: "csv", Location: "a.csv"}},
			Exports: []ExportSpec{{Type: "db"}},
			Options: Options{Workers: -1},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.spec.Validate(); err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
		})
	}
}
func TestSpec_ApplyDefaults(t *testing.T) {
	s := Spec{Options: Options{Workers: 8}}
	s.ApplyDefaults(4, 100, time.Minute)

	if s.Options.Workers != 8 {
		t.Errorf("Workers = %d, want 8 (value from spec must win)", s.Options.Workers)
	}
	if s.Options.BufferSize != 100 {
		t.Errorf("BufferSize = %d, want default 100", s.Options.BufferSize)
	}
	if s.Options.Timeout.Std() != time.Minute {
		t.Errorf("Timeout = %v, want default 1m", s.Options.Timeout.Std())
	}
}
func TestDuration_Invalid(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"ten seconds"`), &d); err == nil {
		t.Fatal("expected error for invalid duration")
	}
	if err := json.Unmarshal([]byte(`10`), &d); err == nil {
		t.Fatal("expected error for a number instead of a string")
	}
}
