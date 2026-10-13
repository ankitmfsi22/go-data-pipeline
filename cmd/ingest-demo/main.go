package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/source"
)

func main() {
	sources := []source.Source{
		&source.CSV{Path: "internal/source/testdata/covid.csv"},
		&source.JSON{Path: "internal/source/testdata/prices.json"},
	}

	if len(os.Args) > 1 {
		sources = append(sources, &source.HTTP{URL: os.Args[1], Timeout: 10 * time.Second})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out := make(chan domain.Record, 10)
	errCh := make(chan error, 1)
	go func() { errCh <- source.ReadAll(ctx, sources, out) }()

	count := 0
	for rec := range out {
		count++
		fmt.Printf("%-3d %-40.40s line %-3d %v\n", count, rec.Source, rec.Line, rec.Fields)
	}

	if err := <-errCh; err != nil {
		fmt.Println("errors:", err)
	}
	fmt.Printf("\ntotal records: %d\n", count)
}
