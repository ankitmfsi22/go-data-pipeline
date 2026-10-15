package export

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

type JSONFile struct {
	Path string
}

func (j JSONFile) Export(_ context.Context, r Result) error {
	return writeFileAtomic(j.Path, func(f *os.File) error {
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	})
}

type CSVFile struct {
	Path string
}

func (c CSVFile) Export(_ context.Context, r Result) error {
	columns := fieldNames(r.Records)
	header := append([]string{"source", "line"}, columns...)

	return writeFileAtomic(c.Path, func(f *os.File) error {
		w := csv.NewWriter(f)
		if err := w.Write(header); err != nil {
			return fmt.Errorf("write header: %w", err)
		}

		for _, rec := range r.Records {
			row := []string{rec.Source, strconv.Itoa(rec.Line)}
			for _, col := range columns {
				v, ok := rec.Fields[col]
				if !ok {
					row = append(row, "")
					continue
				}
				row = append(row, fmt.Sprint(v))
			}
			if err := w.Write(row); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
		}

		w.Flush()
		if err := w.Error(); err != nil {
			return fmt.Errorf("flush csv: %w", err)
		}
		return nil
	})
}

func fieldNames(records []domain.Record) []string {
	seen := map[string]bool{}
	for _, r := range records {
		for k := range r.Fields {
			seen[k] = true
		}
	}
	names := make([]string, 0, len(seen))
	for k := range seen {
		names = append(names, k)
	}
	slices.Sort(names)
	return names
}
