package export

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type Result struct {
	JobID   string             `json:"job_id"`
	Records []domain.Record    `json:"records"`
	Summary []domain.AggResult `json:"summary"`
}

type Exporter interface {
	Export(ctx context.Context, r Result) error
}

func New(spec domain.ExportSpec, saver ResultSaver) (Exporter, error) {
	switch spec.Type {
	case "json":
		return JSONFile{Path: spec.Path}, nil
	case "csv":
		return CSVFile{Path: spec.Path}, nil
	case "db":
		if saver == nil {
			return nil, errors.New("db export needs a database")
		}
		return DB{Store: saver}, nil
	default:
		return nil, fmt.Errorf("unknown export type %q", spec.Type)
	}
}

func Collect(ctx context.Context, in <-chan domain.Record) ([]domain.Record, error) {
	var records []domain.Record
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case rec, ok := <-in:
			if !ok {
				return records, nil
			}
			records = append(records, rec)
		}
	}
}

func writeFileAtomic(path string, write func(f *os.File) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create folder: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()

	if err := write(tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}
