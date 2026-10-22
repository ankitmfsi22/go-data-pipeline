package source

import (
	"context"
	"maps"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type Inline struct {
	Rows []map[string]any
}

func (in *Inline) Name() string { return "inline" }
func (in *Inline) Read(ctx context.Context, out chan<- domain.Record) error {
	for i, row := range in.Rows {
		rec := domain.Record{Source: in.Name(), Line: i + 1, Fields: maps.Clone(row)}
		if err := send(ctx, out, rec); err != nil {
			return err
		}
	}
	return nil
}
