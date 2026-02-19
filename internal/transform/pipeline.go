package transform

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/acme-corp/data-pipeline/internal/ingestion"
)

// Transformer applies a transformation function to records.
// Think of it as a single "step" in an assembly line.
type Transformer struct {
	name string
	fn   TransformFunc
}

// TransformFunc is the signature for any transformation operation.
// It receives a record and returns a modified record, or an error.
type TransformFunc func(ctx context.Context, record ingestion.Record) (ingestion.Record, error)

// Pipeline chains multiple transformers together.
// Records flow through each transformer in sequence — like water
// through a series of filters, each one cleaning or reshaping the data.
type Pipeline struct {
	stages     []*Transformer
	workers    int
	errHandler func(error, ingestion.Record)
	mu         sync.RWMutex
}

// NewPipeline creates a transform pipeline with the given worker count.
func NewPipeline(workers int) *Pipeline {
	return &Pipeline{
		workers: workers,
		errHandler: func(err error, r ingestion.Record) {
			log.Printf("transform error on record %s: %v", r.ID, err)
		},
	}
}

// AddStage appends a named transformer to the pipeline.
func (p *Pipeline) AddStage(name string, fn TransformFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stages = append(p.stages, &Transformer{name: name, fn: fn})
}

// SetErrorHandler sets a custom error handler for failed transformations.
func (p *Pipeline) SetErrorHandler(handler func(error, ingestion.Record)) {
	p.errHandler = handler
}

// Process applies all transform stages to a batch of records using a
// fan-out/fan-in concurrency pattern:
//
//	            ┌──► worker 1 ──┐
//	input ──►───┼──► worker 2 ──┼───► output
//	            └──► worker 3 ──┘
//
// Each worker independently processes records through ALL stages.
// This is safe because each record is independent — no shared state.
func (p *Pipeline) Process(ctx context.Context, batch *ingestion.Batch) (*ingestion.Batch, []error) {
	p.mu.RLock()
	stages := make([]*Transformer, len(p.stages))
	copy(stages, p.stages)
	p.mu.RUnlock()

	if len(stages) == 0 {
		return batch, nil
	}

	type result struct {
		record ingestion.Record
		err    error
		index  int
	}

	input := make(chan struct {
		record ingestion.Record
		index  int
	}, len(batch.Records))
	output := make(chan result, len(batch.Records))

	// Fan-out: launch workers
	var wg sync.WaitGroup
	for w := 0; w < p.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range input {
				rec := item.record
				var err error

				// Apply each stage in sequence
				for _, stage := range stages {
					rec, err = stage.fn(ctx, rec)
					if err != nil {
						err = fmt.Errorf("stage %q: %w", stage.name, err)
						break
					}
				}
				output <- result{record: rec, err: err, index: item.index}
			}
		}()
	}

	// Feed records into input channel
	for i, rec := range batch.Records {
		input <- struct {
			record ingestion.Record
			index  int
		}{record: rec, index: i}
	}
	close(input)

	// Fan-in: collect results
	go func() {
		wg.Wait()
		close(output)
	}()

	transformed := make([]ingestion.Record, len(batch.Records))
	var errors []error
	for res := range output {
		if res.err != nil {
			errors = append(errors, res.err)
			p.errHandler(res.err, res.record)
		} else {
			transformed[res.index] = res.record
		}
	}

	// Filter out zero-value records (failed transforms)
	clean := make([]ingestion.Record, 0, len(transformed))
	for _, rec := range transformed {
		if rec.ID != "" {
			clean = append(clean, rec)
		}
	}

	return &ingestion.Batch{
		Records:   clean,
		Source:    batch.Source,
		CreatedAt: batch.CreatedAt,
		SeqNum:    batch.SeqNum,
	}, errors
}

// ---- Built-in Transform Functions ----

// FilterTransform removes records that don't match a condition.
func FilterTransform(field, operator, value string) TransformFunc {
	return func(ctx context.Context, rec ingestion.Record) (ingestion.Record, error) {
		fieldVal, ok := rec.Data[field]
		if !ok {
			return rec, fmt.Errorf("field %q not found in record", field)
		}

		strVal := fmt.Sprintf("%v", fieldVal)

		var match bool
		switch operator {
		case "eq":
			match = strVal == value
		case "neq":
			match = strVal != value
		case "contains":
			match = strings.Contains(strVal, value)
		case "gt":
			a, _ := strconv.ParseFloat(strVal, 64)
			b, _ := strconv.ParseFloat(value, 64)
			match = a > b
		case "lt":
			a, _ := strconv.ParseFloat(strVal, 64)
			b, _ := strconv.ParseFloat(value, 64)
			match = a < b
		default:
			return rec, fmt.Errorf("unknown operator: %s", operator)
		}

		if !match {
			// Return zero record — will be filtered out in Process()
			return ingestion.Record{}, nil
		}
		return rec, nil
	}
}

// MapTransform renames or computes new fields.
func MapTransform(mappings map[string]string) TransformFunc {
	return func(ctx context.Context, rec ingestion.Record) (ingestion.Record, error) {
		for newField, sourceExpr := range mappings {
			if val, ok := rec.Data[sourceExpr]; ok {
				rec.Data[newField] = val
				if newField != sourceExpr {
					delete(rec.Data, sourceExpr)
				}
			}
		}
		return rec, nil
	}
}

// NormalizeTransform lowercases string fields and trims whitespace.
func NormalizeTransform(fields []string) TransformFunc {
	return func(ctx context.Context, rec ingestion.Record) (ingestion.Record, error) {
		for _, field := range fields {
			if val, ok := rec.Data[field]; ok {
				if strVal, ok := val.(string); ok {
					rec.Data[field] = strings.TrimSpace(strings.ToLower(strVal))
				}
			}
		}
		return rec, nil
	}
}

// DeduplicateTransform marks records as duplicates based on a key field.
// NOTE: This is stateful — it uses a shared map across goroutines.
// BUG: potential race condition if workers > 1.
var seenKeys = make(map[string]bool)
var seenMu sync.Mutex

func DeduplicateTransform(keyField string) TransformFunc {
	return func(ctx context.Context, rec ingestion.Record) (ingestion.Record, error) {
		val, ok := rec.Data[keyField]
		if !ok {
			return rec, nil
		}

		key := fmt.Sprintf("%v", val)
		seenMu.Lock()
		defer seenMu.Unlock()

		if seenKeys[key] {
			rec.Metadata["duplicate"] = "true"
			return ingestion.Record{}, nil // filtered out
		}
		seenKeys[key] = true
		return rec, nil
	}
}
