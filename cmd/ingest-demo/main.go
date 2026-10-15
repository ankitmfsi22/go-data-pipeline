package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/export"
	"github.com/ankitmfsi22/go-data-pipeline/internal/pipeline"
	"github.com/ankitmfsi22/go-data-pipeline/internal/source"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func run() error {
	const jobID = "demo-job"

	sources := []source.Source{
		&source.CSV{Path: "internal/source/testdata/covid.csv"},
		&source.JSON{Path: "internal/source/testdata/prices.json"},
	}

	validator, err := pipeline.NewValidator([]domain.ValidationRule{
		{Field: "cases", Rule: "min", Value: 0},
		{Field: "price_usd", Rule: "min", Value: 0},
	})
	if err != nil {
		return err
	}

	transformer, err := pipeline.NewTransformer([]domain.Transformation{
		{Field: "country", Op: "lowercase"},
		{Field: "cases", Op: "to_number"},
		{Field: "symbol", Op: "uppercase"},
	})
	if err != nil {
		return err
	}

	aggregator, err := pipeline.NewAggregator([]domain.Aggregation{
		{Op: "count"},
		{Op: "sum", Field: "cases", GroupBy: "country"},
		{Op: "avg", Field: "price_usd"},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := store.OpenSQLite(ctx, "output/pipeline.db")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	exporters := []export.Exporter{
		export.JSONFile{Path: "output/report.json"},
		export.CSVFile{Path: "output/report.csv"},
		export.DB{Store: db},
	}

	recordsCh := make(chan domain.Record, 10)
	validatedCh := make(chan domain.Record, 10)
	transformedCh := make(chan domain.Record, 10)
	resultCh := make(chan domain.Record, 10)
	errorCh := make(chan domain.RecordError, 10)
	go func() {
		if err := source.ReadAll(ctx, sources, recordsCh); err != nil {
			fmt.Println("source errors:", err)
		}
	}()
	go pipeline.RunWorkers(ctx, "validate", 3, recordsCh, validatedCh, errorCh, validator.Validate)
	go pipeline.RunWorkers(ctx, "transform", 3, validatedCh, transformedCh, errorCh, transformer.Transform)

	var summary []domain.AggResult
	var aggErr error
	aggDone := make(chan struct{})
	go func() {
		defer close(aggDone)
		summary, aggErr = aggregator.Run(ctx, transformedCh, resultCh)
	}()

	var failed []domain.RecordError
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range errorCh {
			failed = append(failed, e)
		}
	}()

	records, err := export.Collect(ctx, resultCh)
	if err != nil {
		return err
	}
	<-aggDone
	if aggErr != nil {
		return aggErr
	}
	close(errorCh)
	wg.Wait()

	result := export.Result{JobID: jobID, Records: records, Summary: summary}
	for _, e := range exporters {
		if err := e.Export(ctx, result); err != nil {
			return fmt.Errorf("export: %w", err)
		}
	}

	fmt.Printf("passed: %d, rejected: %d\n\n", len(records), len(failed))
	for _, e := range failed {
		fmt.Println("rejected:", e.Error())
	}

	fmt.Println("\n== summary ==")
	for _, a := range summary {
		fmt.Printf("%s(%s) by %q: %v\n", a.Op, a.Field, a.GroupBy, a.Values)
	}

	fmt.Println("\nsaved: output/report.json, output/report.csv, output/pipeline.db")
	return nil
}
