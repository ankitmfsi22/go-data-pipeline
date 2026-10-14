package pipeline

import (
	"context"
	"sync"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type StepFunc func(domain.Record) (domain.Record, error)

func RunWorkers(
	ctx context.Context,
	stage string,
	n int,
	in <-chan domain.Record,
	out chan<- domain.Record,
	errs chan<- domain.RecordError,
	step StepFunc,
) {
	if n < 1 {
		n = 1
	}

	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, stage, in, out, errs, step)
		}()
	}

	wg.Wait()
	close(out)
}

func worker(
	ctx context.Context,
	stage string,
	in <-chan domain.Record,
	out chan<- domain.Record,
	errs chan<- domain.RecordError,
	step StepFunc,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-in:
			if !ok {
				return
			}

			result, err := step(rec)
			if err != nil {
				select {
				case errs <- domain.RecordError{Stage: stage, Record: rec, Reason: err.Error()}:
				case <-ctx.Done():
					return
				}
				continue
			}

			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}
