package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/acme-corp/data-pipeline/internal/ingestion"
)

// Writer defines the interface for writing processed data to a destination.
type Writer interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, batch *ingestion.Batch) error
	Flush(ctx context.Context) error
	Close() error
}

// BufferedWriter accumulates records and flushes them in bulk.
// This is like a mail carrier — instead of delivering one letter at a time,
// they collect a bag full and deliver them all at once. More efficient.
type BufferedWriter struct {
	inner     Writer
	buffer    []ingestion.Record
	bufSize   int
	flushInterval time.Duration
	mu        sync.Mutex
	done      chan struct{}
	flushErr  error
}

// NewBufferedWriter wraps any Writer with buffering and periodic flushing.
func NewBufferedWriter(inner Writer, bufSize int, flushInterval time.Duration) *BufferedWriter {
	return &BufferedWriter{
		inner:         inner,
		buffer:        make([]ingestion.Record, 0, bufSize),
		bufSize:       bufSize,
		flushInterval: flushInterval,
		done:          make(chan struct{}),
	}
}

func (bw *BufferedWriter) Open(ctx context.Context) error {
	if err := bw.inner.Open(ctx); err != nil {
		return err
	}

	// Start periodic flush goroutine
	go bw.periodicFlush(ctx)
	return nil
}

func (bw *BufferedWriter) Write(ctx context.Context, batch *ingestion.Batch) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.buffer = append(bw.buffer, batch.Records...)

	if len(bw.buffer) >= bw.bufSize {
		return bw.flushLocked(ctx)
	}
	return nil
}

func (bw *BufferedWriter) Flush(ctx context.Context) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushLocked(ctx)
}

func (bw *BufferedWriter) flushLocked(ctx context.Context) error {
	if len(bw.buffer) == 0 {
		return nil
	}

	batch := &ingestion.Batch{
		Records:   bw.buffer,
		CreatedAt: time.Now(),
	}

	if err := bw.inner.Write(ctx, batch); err != nil {
		bw.flushErr = err
		return fmt.Errorf("flushing buffer: %w", err)
	}

	bw.buffer = make([]ingestion.Record, 0, bw.bufSize)
	return nil
}

func (bw *BufferedWriter) periodicFlush(ctx context.Context) {
	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := bw.Flush(ctx); err != nil {
				log.Printf("periodic flush error: %v", err)
			}
		case <-bw.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (bw *BufferedWriter) Close() error {
	close(bw.done)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = bw.Flush(ctx)
	return bw.inner.Close()
}

// ---- File-based Writer Implementation ----

// JSONFileWriter writes records as newline-delimited JSON (NDJSON).
// Used for local development and testing.
type JSONFileWriter struct {
	path string
	file *os.File
	mu   sync.Mutex
}

func NewJSONFileWriter(path string) *JSONFileWriter {
	return &JSONFileWriter{path: path}
}

func (w *JSONFileWriter) Open(ctx context.Context) error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening output file: %w", err)
	}
	w.file = f
	return nil
}

func (w *JSONFileWriter) Write(ctx context.Context, batch *ingestion.Batch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range batch.Records {
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshaling record %s: %w", rec.ID, err)
		}
		if _, err := w.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("writing record %s: %w", rec.ID, err)
		}
	}
	return nil
}

func (w *JSONFileWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

func (w *JSONFileWriter) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// ---- Retry Wrapper ----

// RetryWriter wraps a Writer with exponential backoff retry logic.
type RetryWriter struct {
	inner       Writer
	maxRetries  int
	baseDelay   time.Duration
	deadLetterFn func(ingestion.Batch, error) // callback for failed batches
}

func NewRetryWriter(inner Writer, maxRetries int, baseDelay time.Duration) *RetryWriter {
	return &RetryWriter{
		inner:      inner,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
	}
}

func (rw *RetryWriter) SetDeadLetterHandler(fn func(ingestion.Batch, error)) {
	rw.deadLetterFn = fn
}

func (rw *RetryWriter) Open(ctx context.Context) error {
	return rw.inner.Open(ctx)
}

func (rw *RetryWriter) Write(ctx context.Context, batch *ingestion.Batch) error {
	var lastErr error

	for attempt := 0; attempt <= rw.maxRetries; attempt++ {
		if err := rw.inner.Write(ctx, batch); err != nil {
			lastErr = err
			delay := rw.baseDelay * time.Duration(1<<uint(attempt))
			log.Printf("write attempt %d/%d failed: %v, retrying in %v",
				attempt+1, rw.maxRetries+1, err, delay)

			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil // success
	}

	// All retries exhausted — send to dead letter queue
	if rw.deadLetterFn != nil {
		rw.deadLetterFn(*batch, lastErr)
	}
	return fmt.Errorf("write failed after %d attempts: %w", rw.maxRetries+1, lastErr)
}

func (rw *RetryWriter) Flush(ctx context.Context) error {
	return rw.inner.Flush(ctx)
}

func (rw *RetryWriter) Close() error {
	return rw.inner.Close()
}
