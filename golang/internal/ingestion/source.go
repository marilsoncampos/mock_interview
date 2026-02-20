package ingestion

import (
	"context"
	"time"
)

// Record represents a single data record flowing through the pipeline.
// Think of it as one row in a table — a map of column names to values.
type Record struct {
	ID        string
	Source    string
	Timestamp time.Time
	Data      map[string]interface{}
	Metadata  map[string]string
}

// Batch is a collection of records processed together as a unit.
type Batch struct {
	Records   []Record
	Source    string
	CreatedAt time.Time
	SeqNum    int64
}

// Source defines the interface all data sources must implement.
// This is the "plug" side of a plugin architecture — each source type
// (CSV, API, database) implements this to feed data into the pipeline.
type Source interface {
	// Name returns a human-readable identifier for logging/metrics.
	Name() string

	// Open initializes the source connection or file handle.
	Open(ctx context.Context) error

	// ReadBatch returns the next batch of records.
	// Returns io.EOF when no more data is available.
	ReadBatch(ctx context.Context, size int) (*Batch, error)

	// Close releases any resources held by the source.
	Close() error

	// Checkpoint returns opaque state for resumable reads.
	Checkpoint() ([]byte, error)
}
