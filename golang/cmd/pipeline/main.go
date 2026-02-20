package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/acme-corp/data-pipeline/internal/config"
	"github.com/acme-corp/data-pipeline/internal/ingestion"
	"github.com/acme-corp/data-pipeline/internal/metrics"
	"github.com/acme-corp/data-pipeline/internal/storage"
	"github.com/acme-corp/data-pipeline/internal/transform"
)

func main() {
	configPath := flag.String("config", "pipeline.json", "path to pipeline config")
	dryRun := flag.Bool("dry-run", false, "validate config and exit")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Loaded config with %d sources, %d transforms", len(cfg.Sources), len(cfg.Transforms))

	if *dryRun {
		fmt.Println("Config validation passed.")
		os.Exit(0)
	}

	// Setup graceful shutdown — catch Ctrl+C and SIGTERM.
	// Think of this like a factory shutdown procedure: stop accepting new
	// material, finish processing what's in the pipeline, then power down.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Initialize metrics
	collector := metrics.NewCollector()

	// Initialize sources
	sources, err := initSources(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize sources: %v", err)
	}
	defer closeSources(sources)

	// Initialize transform pipeline
	transformPipeline := initTransforms(cfg)

	// Initialize writer with buffering and retry
	writer := initWriter(cfg)
	if err := writer.Open(ctx); err != nil {
		log.Fatalf("Failed to open writer: %v", err)
	}
	defer writer.Close()

	// Start metrics reporter
	go reportMetrics(ctx, collector)

	// Run the pipeline
	runPipeline(ctx, cfg, sources, transformPipeline, writer, collector)

	// Final metrics report
	snap, _ := collector.JSON()
	log.Printf("Final metrics:\n%s", snap)
}

func initSources(ctx context.Context, cfg *config.PipelineConfig) ([]ingestion.Source, error) {
	var sources []ingestion.Source
	for _, srcCfg := range cfg.Sources {
		var src ingestion.Source
		switch srcCfg.Type {
		case "csv":
			src = ingestion.NewCSVSource(srcCfg.Name, srcCfg.Location)
		default:
			return nil, fmt.Errorf("unsupported source type: %s", srcCfg.Type)
		}

		if err := src.Open(ctx); err != nil {
			return nil, fmt.Errorf("opening source %s: %w", srcCfg.Name, err)
		}
		sources = append(sources, src)
	}
	return sources, nil
}

func closeSources(sources []ingestion.Source) {
	for _, src := range sources {
		if err := src.Close(); err != nil {
			log.Printf("Error closing source %s: %v", src.Name(), err)
		}
	}
}

func initTransforms(cfg *config.PipelineConfig) *transform.Pipeline {
	tp := transform.NewPipeline(cfg.Pipeline.Workers)

	for _, rule := range cfg.Transforms {
		switch rule.Operation {
		case "filter":
			field, _ := rule.Config["field"].(string)
			op, _ := rule.Config["operator"].(string)
			val, _ := rule.Config["value"].(string)
			tp.AddStage(rule.Name, transform.FilterTransform(field, op, val))

		case "normalize":
			fieldsRaw, _ := rule.Config["fields"].([]interface{})
			fields := make([]string, len(fieldsRaw))
			for i, f := range fieldsRaw {
				fields[i], _ = f.(string)
			}
			tp.AddStage(rule.Name, transform.NormalizeTransform(fields))

		case "deduplicate":
			keyField, _ := rule.Config["key_field"].(string)
			tp.AddStage(rule.Name, transform.DeduplicateTransform(keyField))

		default:
			log.Printf("Warning: unknown transform operation %q, skipping", rule.Operation)
		}
	}

	return tp
}

func initWriter(cfg *config.PipelineConfig) storage.Writer {
	var baseWriter storage.Writer

	switch cfg.Destination.Type {
	case "jsonfile":
		baseWriter = storage.NewJSONFileWriter(cfg.Destination.ConnectionString)
	default:
		log.Fatalf("Unsupported destination type: %s", cfg.Destination.Type)
	}

	// Wrap with retry logic
	retryWriter := storage.NewRetryWriter(
		baseWriter,
		cfg.Destination.RetryAttempts,
		cfg.Destination.RetryDelay,
	)

	// Wrap with buffering
	return storage.NewBufferedWriter(
		retryWriter,
		cfg.Pipeline.BufferSize,
		cfg.Pipeline.FlushInterval,
	)
}

// runPipeline orchestrates the main processing loop.
// It reads from all sources concurrently, transforms, and writes.
//
//    sources ──► channel ──► transform workers ──► buffered writer
//
// The channel acts as a shock absorber between fast producers (sources)
// and potentially slower consumers (transforms + writer).
func runPipeline(
	ctx context.Context,
	cfg *config.PipelineConfig,
	sources []ingestion.Source,
	tp *transform.Pipeline,
	writer storage.Writer,
	collector *metrics.Collector,
) {
	batchChan := make(chan *ingestion.Batch, cfg.Pipeline.BufferSize)

	// Source goroutines: each source reads independently and pushes batches
	var sourceWg sync.WaitGroup
	for i, src := range sources {
		sourceWg.Add(1)
		batchSize := cfg.Sources[i].BatchSize

		go func(s ingestion.Source, bs int) {
			defer sourceWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				batch, err := s.ReadBatch(ctx, bs)
				if err == io.EOF {
					log.Printf("Source %s exhausted", s.Name())
					return
				}
				if err != nil {
					log.Printf("Error reading from %s: %v", s.Name(), err)
					collector.RecordFailed(1)
					continue
				}

				collector.RecordRead(int64(len(batch.Records)))
				collector.BatchProcessed()

				select {
				case batchChan <- batch:
				case <-ctx.Done():
					return
				}
			}
		}(src, batchSize)
	}

	// Close batch channel when all sources are done
	go func() {
		sourceWg.Wait()
		close(batchChan)
	}()

	// Process batches: transform and write
	var processWg sync.WaitGroup
	for i := 0; i < cfg.Pipeline.Workers; i++ {
		processWg.Add(1)
		go func(workerID int) {
			defer processWg.Done()
			for batch := range batchChan {
				start := time.Now()

				// Transform
				transformed, errs := tp.Process(ctx, batch)
				collector.TrackStageDuration("transform", time.Since(start))

				if len(errs) > 0 {
					collector.RecordFailed(int64(len(errs)))
				}

				filtered := int64(len(batch.Records) - len(transformed.Records))
				if filtered > 0 {
					collector.RecordFiltered(filtered)
				}

				// Write
				writeStart := time.Now()
				if err := writer.Write(ctx, transformed); err != nil {
					log.Printf("Worker %d: write error: %v", workerID, err)
					collector.RecordFailed(int64(len(transformed.Records)))
					continue
				}
				collector.TrackStageDuration("write", time.Since(writeStart))
				collector.RecordWritten(int64(len(transformed.Records)))
			}
		}(i)
	}

	processWg.Wait()

	// Final flush
	if err := writer.Flush(ctx); err != nil {
		log.Printf("Final flush error: %v", err)
	}
}

func reportMetrics(ctx context.Context, collector *metrics.Collector) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snap := collector.Snapshot()
			log.Printf("Pipeline stats — read: %d, written: %d, failed: %d, filtered: %d, throughput: %.1f rec/s",
				snap.RecordsRead, snap.RecordsWritten, snap.RecordsFailed, snap.RecordsFiltered, snap.Throughput)
		case <-ctx.Done():
			return
		}
	}
}
