package pipeline

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

// Supported validation rules.
const (
	RuleRequired = "required"
	RuleNumber   = "number"
	RuleMin      = "min"
	RuleMax      = "max"
)

type Validator struct {
	rules []domain.ValidationRule
}

func NewValidator(rules []domain.ValidationRule) (*Validator, error) {
	for i, r := range rules {
		if r.Field == "" {
			return nil, fmt.Errorf("validation[%d]: field is required", i)
		}
		switch r.Rule {
		case RuleRequired, RuleNumber:
		case RuleMin, RuleMax:
			if _, ok := toFloat(r.Value); !ok {
				return nil, fmt.Errorf("validation[%d]: %s needs a number value", i, r.Rule)
			}
		default:
			return nil, fmt.Errorf("validation[%d]: unknown rule %q", i, r.Rule)
		}
	}
	return &Validator{rules: rules}, nil
}
func (v *Validator) Validate(rec domain.Record) (domain.Record, error) {
	var problems []string
	for _, r := range v.rules {
		if err := check(r, rec.Fields[r.Field]); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) > 0 {
		return rec, errors.New(strings.Join(problems, "; "))
	}
	return rec, nil
}

func check(r domain.ValidationRule, value any) error {
	if r.Rule == RuleRequired {
		if isEmpty(value) {
			return fmt.Errorf("%s is required", r.Field)
		}
		return nil
	}

	if isEmpty(value) {
		return nil
	}
	n, ok := toFloat(value)
	if !ok {
		return fmt.Errorf("%s must be a number, got %q", r.Field, fmt.Sprint(value))
	}

	limit, _ := toFloat(r.Value)
	switch r.Rule {
	case RuleMin:
		if n < limit {
			return fmt.Errorf("%s must be >= %v, got %v", r.Field, limit, n)
		}
	case RuleMax:
		if n > limit {
			return fmt.Errorf("%s must be <= %v, got %v", r.Field, limit, n)
		}
	}
	return nil
}
