package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	SourceCSV  = "csv"
	SourceJSON = "json"
	SourceHTTP = "http"
)

type Spec struct {
	Name            string           `json:"name"`
	Sources         []SourceSpec     `json:"sources"`
	Validation      []ValidationRule `json:"validation"`
	Transformations []Transformation `json:"transformations"`
	Aggregations    []Aggregation    `json:"aggregations"`
	Exports         []ExportSpec     `json:"exports"`
	Options         Options          `json:"options"`
}

type SourceSpec struct {
	Type     string   `json:"type"`
	Location string   `json:"location"`
	Timeout  Duration `json:"timeout"`
}

type ValidationRule struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Value any    `json:"value,omitempty"`
}

type Transformation struct {
	Field string `json:"field"`
	Op    string `json:"op"`
}

type Aggregation struct {
	Op      string `json:"op"`
	Field   string `json:"field,omitempty"`
	GroupBy string `json:"group_by,omitempty"`
}

type ExportSpec struct {
	Type string `json:"type"`
	Path string `json:"path,omitempty"`
}

type Options struct {
	Workers    int      `json:"workers"`
	BufferSize int      `json:"buffer_size"`
	Timeout    Duration `json:"timeout"`
}

func (s *Spec) Validate() error {
	var errs []error

	if len(s.Sources) == 0 {
		errs = append(errs, errors.New("at least one source is required"))
	}
	for i, src := range s.Sources {
		switch src.Type {
		case SourceCSV, SourceJSON, SourceHTTP:
		default:
			errs = append(errs, fmt.Errorf("sources[%d]: unknown type %q (want csv, json or http)", i, src.Type))
		}
		if src.Location == "" {
			errs = append(errs, fmt.Errorf("sources[%d]: location is required", i))
		}
	}

	if len(s.Exports) == 0 {
		errs = append(errs, errors.New("at least one export is required"))
	}
	for i, exp := range s.Exports {
		switch exp.Type {
		case "db":
		case "csv", "json":
			if exp.Path == "" {
				errs = append(errs, fmt.Errorf("exports[%d]: path is required for %s export", i, exp.Type))
			}
		default:
			errs = append(errs, fmt.Errorf("exports[%d]: unknown type %q (want db, csv or json)", i, exp.Type))
		}
	}

	if s.Options.Workers < 0 {
		errs = append(errs, errors.New("options.workers must be >= 0"))
	}
	if s.Options.BufferSize < 0 {
		errs = append(errs, errors.New("options.buffer_size must be >= 0"))
	}

	return errors.Join(errs...)
}
func (s *Spec) ApplyDefaults(workers, bufferSize int, timeout time.Duration) {
	if s.Options.Workers == 0 {
		s.Options.Workers = workers
	}
	if s.Options.BufferSize == 0 {
		s.Options.BufferSize = bufferSize
	}
	if s.Options.Timeout == 0 {
		s.Options.Timeout = Duration(timeout)
	}
}

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	if s == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}
func (d Duration) Std() time.Duration { return time.Duration(d) }
