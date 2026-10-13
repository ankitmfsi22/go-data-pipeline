package source

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type Source interface {
	Name() string
	Read(ctx context.Context, out chan<- domain.Record) error
}

func New(spec domain.SourceSpec, client *http.Client) (Source, error) {
	switch spec.Type {
	case domain.SourceCSV:
		return &CSV{Path: spec.Location}, nil
	case domain.SourceJSON:
		return &JSON{Path: spec.Location}, nil
	case domain.SourceHTTP:
		return &HTTP{URL: spec.Location, Timeout: spec.Timeout.Std(), Client: client}, nil
	default:
		return nil, fmt.Errorf("unknown source type %q", spec.Type)
	}
}
func ReadAll(ctx context.Context, sources []Source, out chan<- domain.Record) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, src := range sources {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()
			if err := s.Read(ctx, out); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("source %s: %w", s.Name(), err))
				mu.Unlock()
			}
		}(src)
	}

	wg.Wait()
	close(out)

	return errors.Join(errs...)
}

func send(ctx context.Context, out chan<- domain.Record, rec domain.Record) error {
	select {
	case out <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
