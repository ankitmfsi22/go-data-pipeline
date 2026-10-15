package export

import (
	"context"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type ResultSaver interface {
	SaveResults(ctx context.Context, jobID string, records []domain.Record, summary []domain.AggResult) error
}
type DB struct {
	Store ResultSaver
}

func (d DB) Export(ctx context.Context, r Result) error {
	return d.Store.SaveResults(ctx, r.JobID, r.Records, r.Summary)
}
