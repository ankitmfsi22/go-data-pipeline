package pipeline

import (
	"context"
	"fmt"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

const (
	AggCount = "count"
	AggSum   = "sum"
	AggAvg   = "avg"
	AggMin   = "min"
	AggMax   = "max"
)
const allGroup = "all"

type Aggregator struct {
	specs []domain.Aggregation
}

func NewAggregator(specs []domain.Aggregation) (*Aggregator, error) {
	for i, s := range specs {
		switch s.Op {
		case AggCount:
		case AggSum, AggAvg, AggMin, AggMax:
			if s.Field == "" {
				return nil, fmt.Errorf("aggregations[%d]: %s needs a field", i, s.Op)
			}
		default:
			return nil, fmt.Errorf("aggregations[%d]: unknown op %q", i, s.Op)
		}
	}
	return &Aggregator{specs: specs}, nil
}

type stats struct {
	count    int
	sum      float64
	min, max float64
}

func (s *stats) add(v float64) {
	if s.count == 0 || v < s.min {
		s.min = v
	}
	if s.count == 0 || v > s.max {
		s.max = v
	}
	s.count++
	s.sum += v
}

func (a *Aggregator) Run(ctx context.Context, in <-chan domain.Record, out chan<- domain.Record) ([]domain.AggResult, error) {
	defer close(out)
	groups := make([]map[string]*stats, len(a.specs))
	for i := range groups {
		groups[i] = map[string]*stats{}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case rec, ok := <-in:
			if !ok {
				return a.results(groups), nil
			}

			for i, s := range a.specs {
				a.add(groups[i], s, rec)
			}

			select {
			case out <- rec:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}

func (a *Aggregator) add(groups map[string]*stats, s domain.Aggregation, rec domain.Record) {
	key := allGroup
	if s.GroupBy != "" {
		g, ok := rec.Fields[s.GroupBy]
		if !ok || isEmpty(g) {
			return
		}
		key = fmt.Sprint(g)
	}

	v := 1.0
	if s.Op != AggCount {
		if s.Field == "" {
			return
		}
		n, ok := toFloat(rec.Fields[s.Field])
		if !ok {
			return
		}
		v = n
	}

	if groups[key] == nil {
		groups[key] = &stats{}
	}
	groups[key].add(v)
}

func (a *Aggregator) results(groups []map[string]*stats) []domain.AggResult {
	out := make([]domain.AggResult, len(a.specs))
	for i, s := range a.specs {
		values := make(map[string]float64, len(groups[i]))
		for key, st := range groups[i] {
			switch s.Op {
			case AggCount:
				values[key] = float64(st.count)
			case AggSum:
				values[key] = st.sum
			case AggAvg:
				values[key] = st.sum / float64(st.count)
			case AggMin:
				values[key] = st.min
			case AggMax:
				values[key] = st.max
			}
		}
		out[i] = domain.AggResult{Op: s.Op, Field: s.Field, GroupBy: s.GroupBy, Values: values}
	}
	return out
}
