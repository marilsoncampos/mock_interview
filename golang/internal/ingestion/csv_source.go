package ingestion

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// CSVSource reads records from CSV files. It implements the Source interface
// and supports batched reading with checkpoint/resume capability.
type CSVSource struct {
	name     string
	path     string
	reader   *csv.Reader
	file     *os.File
	headers  []string
	offset   int64
	seqNum   int64
	mu       sync.Mutex
}

// NewCSVSource creates a new CSV data source.
func NewCSVSource(name, path string) *CSVSource {
	return &CSVSource{
		name: name,
		path: path,
	}
}

func (s *CSVSource) Name() string { return s.name }

func (s *CSVSource) Open(ctx context.Context) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("opening csv %s: %w", s.path, err)
	}
	s.file = f
	s.reader = csv.NewReader(f)
	s.reader.LazyQuotes = true
	s.reader.TrimLeadingSpace = true

	// First row is always headers
	headers, err := s.reader.Read()
	if err != nil {
		f.Close()
		return fmt.Errorf("reading csv headers: %w", err)
	}
	s.headers = headers
	return nil
}

func (s *CSVSource) ReadBatch(ctx context.Context, size int) (*Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]Record, 0, size)

	for i := 0; i < size; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := s.reader.Read()
		if err == io.EOF {
			if len(records) > 0 {
				break // return what we have
			}
			return nil, io.EOF
		}
		if err != nil {
			// BUG: We skip malformed rows silently. In production,
			// these should go to a dead letter queue.
			continue
		}

		data := make(map[string]interface{}, len(s.headers))
		for j, header := range s.headers {
			if j < len(row) {
				data[header] = row[j]
			}
		}

		s.offset++
		records = append(records, Record{
			ID:        fmt.Sprintf("%s-%d", s.name, s.offset),
			Source:    s.name,
			Timestamp: time.Now(),
			Data:      data,
			Metadata: map[string]string{
				"row_number": fmt.Sprintf("%d", s.offset),
				"file":       s.path,
			},
		})
	}

	s.seqNum++
	return &Batch{
		Records:   records,
		Source:    s.name,
		CreatedAt: time.Now(),
		SeqNum:    s.seqNum,
	}, nil
}

func (s *CSVSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *CSVSource) Checkpoint() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []byte(fmt.Sprintf("%d", s.offset)), nil
}
