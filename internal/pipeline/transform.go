package pipeline

import (
	"fmt"
	"maps"
	"strings"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

// Supported transformation ops.
const (
	OpTrim      = "trim"
	OpLowercase = "lowercase"
	OpUppercase = "uppercase"
	OpToNumber  = "to_number"
)

type Transformer struct {
	steps []domain.Transformation
}

func NewTransformer(steps []domain.Transformation) (*Transformer, error) {
	for i, s := range steps {
		if s.Field == "" {
			return nil, fmt.Errorf("transformations[%d]: field is required", i)
		}
		switch s.Op {
		case OpTrim, OpLowercase, OpUppercase, OpToNumber:
		default:
			return nil, fmt.Errorf("transformations[%d]: unknown op %q", i, s.Op)
		}
	}
	return &Transformer{steps: steps}, nil
}

func (t *Transformer) Transform(rec domain.Record) (domain.Record, error) {
	fields := maps.Clone(rec.Fields)

	for _, s := range t.steps {
		v, ok := fields[s.Field]
		if !ok {
			continue
		}

		switch s.Op {
		case OpTrim:
			fields[s.Field] = strings.TrimSpace(fmt.Sprint(v))
		case OpLowercase:
			fields[s.Field] = strings.ToLower(strings.TrimSpace(fmt.Sprint(v)))
		case OpUppercase:
			fields[s.Field] = strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
		case OpToNumber:
			n, ok := toFloat(v)
			if !ok {
				return rec, fmt.Errorf("%s: cannot convert %q to a number", s.Field, fmt.Sprint(v))
			}
			fields[s.Field] = n
		}
	}

	rec.Fields = fields
	return rec, nil
}
