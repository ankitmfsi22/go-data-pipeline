package source

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type CSV struct {
	Path string
}

func (c *CSV) Name() string { return filepath.Base(c.Path) }

func (c *CSV) Read(ctx context.Context, out chan<- domain.Record) error {
	f, err := os.Open(c.Path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("file is empty")
		}
		return fmt.Errorf("read header: %w", err)
	}
	for i := range header {
		header[i] = strings.TrimSpace(header[i])
	}

	line := 1
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		line++
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}

		fields := make(map[string]any, len(header))
		for i, name := range header {
			if i < len(row) {
				fields[name] = row[i]
			} else {
				fields[name] = ""
			}
		}

		rec := domain.Record{Source: c.Name(), Line: line, Fields: fields}
		if err := send(ctx, out, rec); err != nil {
			return err
		}
	}
}
