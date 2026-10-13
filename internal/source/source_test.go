package source

import (
	"context"
	"net/http"
	"testing"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func collect(t *testing.T, src Source) ([]domain.Record, error) {
	t.Helper()
	out := make(chan domain.Record)
	errCh := make(chan error, 1)

	go func() {
		errCh <- src.Read(t.Context(), out)
		close(out)
	}()

	var recs []domain.Record
	for r := range out {
		recs = append(recs, r)
	}
	return recs, <-errCh
}

func TestNew(t *testing.T) {
	tests := []struct {
		spec    domain.SourceSpec
		wantErr bool
	}{
		{domain.SourceSpec{Type: "csv", Location: "a.csv"}, false},
		{domain.SourceSpec{Type: "json", Location: "a.json"}, false},
		{domain.SourceSpec{Type: "http", Location: "https://example.com"}, false},
		{domain.SourceSpec{Type: "xml", Location: "a.xml"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.spec.Type, func(t *testing.T) {
			_, err := New(tt.spec, http.DefaultClient)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New(%q) error = %v, wantErr %v", tt.spec.Type, err, tt.wantErr)
			}
		})
	}
}
func TestReadAll_MultipleSources(t *testing.T) {
	sources := []Source{
		&CSV{Path: "testdata/covid.csv"},
		&JSON{Path: "testdata/prices.json"},
		&CSV{Path: "testdata/missing.csv"},
	}
	out := make(chan domain.Record, 10)

	errCh := make(chan error, 1)
	go func() { errCh <- ReadAll(t.Context(), sources, out) }()

	count := 0
	for range out {
		count++
	}

	if count != 8 {
		t.Errorf("got %d records, want 8 (5 csv + 3 json)", count)
	}
	if err := <-errCh; err == nil {
		t.Error("ReadAll error = nil, want error for missing.csv")
	}
}

func TestReadAll_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan domain.Record)

	errCh := make(chan error, 1)
	go func() { errCh <- ReadAll(ctx, []Source{&CSV{Path: "testdata/covid.csv"}}, out) }()

	cancel()

	if err := <-errCh; err == nil {
		t.Fatal("ReadAll error = nil, want context.Canceled")
	}
}
