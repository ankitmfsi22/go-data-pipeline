package source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type JSON struct {
	Path string
}

func (j *JSON) Name() string { return filepath.Base(j.Path) }
func (j *JSON) Read(ctx context.Context, out chan<- domain.Record) error {
	f, err := os.Open(j.Path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := json.NewDecoder(f)
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("read start of array: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected a JSON array, got %v", tok)
	}

	line := 0
	for dec.More() {
		line++
		var fields map[string]any
		if err := dec.Decode(&fields); err != nil {
			return fmt.Errorf("item %d: %w", line, err)
		}
		rec := domain.Record{Source: j.Name(), Line: line, Fields: fields}
		if err := send(ctx, out, rec); err != nil {
			return err
		}
	}
	return nil
}
