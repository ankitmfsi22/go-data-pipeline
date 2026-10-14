package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/pipeline"
	"github.com/ankitmfsi22/go-data-pipeline/internal/source"
)

func main() {
	sources := []source.Source{
		&source.CSV{Path: "internal/source/testdata/covid.csv"},
		&source.JSON{Path: "internal/source/testdata/prices.json"},
	}

	validator, err := pipeline.NewValidator([]domain.ValidationRule{
		{Field: "cases", Rule: "min", Value: 0},
		{Field: "price_usd", Rule: "min", Value: 0},
	})
	if err != nil {
		fmt.Println("bad rules:", err)
		os.Exit(1)
	}

	transformer, err := pipeline.NewTransformer([]domain.Transformation{
		{Field: "country", Op: "lowercase"},
		{Field: "cases", Op: "to_number"},
		{Field: "symbol", Op: "uppercase"},
	})
	if err != nil {
		fmt.Println("bad transformations:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	recordsCh := make(chan domain.Record, 10)
	validatedCh := make(chan domain.Record, 10)
	transformedCh := make(chan domain.Record, 10)
	errorCh := make(chan domain.RecordError, 10)
	go func() {
		if err := source.ReadAll(ctx, sources, recordsCh); err != nil {
			fmt.Println("source errors:", err)
		}
	}()
	go pipeline.RunWorkers(ctx, "validate", 3, recordsCh, validatedCh, errorCh, validator.Validate)
	go pipeline.RunWorkers(ctx, "transform", 3, validatedCh, transformedCh, errorCh, transformer.Transform)
	var failed []domain.RecordError
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range errorCh {
			failed = append(failed, e)
		}
	}()

	fmt.Println("== passed ==")
	count := 0
	for rec := range transformedCh {
		count++
		fmt.Printf("%-12s line %-2d %v\n", rec.Source, rec.Line, rec.Fields)
	}
	close(errorCh)
	wg.Wait()

	fmt.Println("\n== rejected ==")
	for _, e := range failed {
		fmt.Println(e.Error())
	}

	fmt.Printf("\npassed: %d, rejected: %d\n", count, len(failed))
}
