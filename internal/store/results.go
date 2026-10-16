package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func (s *SQLite) GetSummary(ctx context.Context, jobID string) ([]domain.AggResult, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT op, field, group_by, group_key, value FROM job_summaries WHERE job_id = ? ORDER BY id`, jobID)
	if err != nil {
		return nil, fmt.Errorf("get summary: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []domain.AggResult{}
	index := map[string]int{}
	for rows.Next() {
		var a domain.AggResult
		var key string
		var value float64
		if err := rows.Scan(&a.Op, &a.Field, &a.GroupBy, &key, &value); err != nil {
			return nil, err
		}
		id := a.Op + "|" + a.Field + "|" + a.GroupBy
		i, ok := index[id]
		if !ok {
			a.Values = map[string]float64{}
			out = append(out, a)
			i = len(out) - 1
			index[id] = i
		}
		out[i].Values[key] = value
	}
	return out, rows.Err()
}
func (s *SQLite) ListRecords(ctx context.Context, jobID string, limit, offset int) ([]domain.Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT source, line, data FROM job_records WHERE job_id = ? ORDER BY source, line LIMIT ? OFFSET ?`,
		jobID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list records: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []domain.Record{}
	for rows.Next() {
		var r domain.Record
		var data string
		if err := rows.Scan(&r.Source, &r.Line, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(data), &r.Fields); err != nil {
			return nil, fmt.Errorf("decode record: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
