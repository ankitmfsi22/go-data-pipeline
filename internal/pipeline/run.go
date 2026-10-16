package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/export"
	"github.com/ankitmfsi22/go-data-pipeline/internal/source"
)

type Observer interface {
	Observe(stage string, took time.Duration, failed bool)
}

type Deps struct {
	HTTPClient *http.Client
	Saver      export.ResultSaver
}

type Plan struct {
	spec        domain.Spec
	sources     []source.Source
	validator   *Validator
	transformer *Transformer
	aggregator  *Aggregator
	exporters   []export.Exporter
}

type Output struct {
	Records int
	Summary []domain.AggResult
	Errors  []domain.RecordError
}

func NewPlan(spec domain.Spec, deps Deps) (*Plan, error) {
	p := &Plan{spec: spec}
	var err error

	for i, s := range spec.Sources {
		src, err := source.New(s, deps.HTTPClient)
		if err != nil {
			return nil, fmt.Errorf("sources[%d]: %w", i, err)
		}
		p.sources = append(p.sources, src)
	}
	if p.validator, err = NewValidator(spec.Validation); err != nil {
		return nil, err
	}
	if p.transformer, err = NewTransformer(spec.Transformations); err != nil {
		return nil, err
	}
	if p.aggregator, err = NewAggregator(spec.Aggregations); err != nil {
		return nil, err
	}
	for i, e := range spec.Exports {
		exp, err := export.New(e, deps.Saver)
		if err != nil {
			return nil, fmt.Errorf("exports[%d]: %w", i, err)
		}
		p.exporters = append(p.exporters, exp)
	}
	return p, nil
}

func (p *Plan) Run(ctx context.Context, jobID string, obs Observer) (Output, error) {
	n, buf := p.spec.Options.Workers, p.spec.Options.BufferSize

	recordsCh := make(chan domain.Record, buf)
	validatedCh := make(chan domain.Record, buf)
	transformedCh := make(chan domain.Record, buf)
	resultCh := make(chan domain.Record, buf)
	errorCh := make(chan domain.RecordError, buf)

	readErr := make(chan error, 1)
	go func() { readErr <- source.ReadAll(ctx, p.sources, recordsCh) }()

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		RunWorkers(ctx, "validate", n, recordsCh, validatedCh, errorCh, timed("validate", obs, p.validator.Validate))
	}()
	go func() {
		defer workers.Done()
		RunWorkers(ctx, "transform", n, validatedCh, transformedCh, errorCh, timed("transform", obs, p.transformer.Transform))
	}()

	var summary []domain.AggResult
	aggErr := make(chan error, 1)
	go func() {
		var err error
		summary, err = p.aggregator.Run(ctx, transformedCh, resultCh)
		aggErr <- err
	}()

	var failed []domain.RecordError
	errsDone := make(chan struct{})
	go func() {
		defer close(errsDone)
		for e := range errorCh {
			failed = append(failed, e)
		}
	}()

	records, collectErr := export.Collect(ctx, resultCh)
	aggregateErr := <-aggErr
	workers.Wait()
	close(errorCh)
	<-errsDone
	ingestErr := <-readErr

	out := Output{Records: len(records), Summary: summary, Errors: failed}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := errors.Join(ingestErr, aggregateErr, collectErr); err != nil {
		return out, err
	}

	result := export.Result{JobID: jobID, Records: records, Summary: summary}
	for _, e := range p.exporters {
		if err := e.Export(ctx, result); err != nil {
			return out, fmt.Errorf("export: %w", err)
		}
	}
	return out, nil
}
func timed(stage string, obs Observer, step StepFunc) StepFunc {
	if obs == nil {
		return step
	}
	return func(rec domain.Record) (domain.Record, error) {
		start := time.Now()
		out, err := step(rec)
		obs.Observe(stage, time.Since(start), err != nil)
		return out, err
	}
}
