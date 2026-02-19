package metrics

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Collector gathers pipeline metrics using atomic counters for lock-free
// concurrent updates. Think of it as the dashboard gauges on a factory floor —
// each worker updates its own counter, and the collector shows the totals.
type Collector struct {
	recordsRead      atomic.Int64
	recordsWritten   atomic.Int64
	recordsFailed    atomic.Int64
	recordsFiltered  atomic.Int64
	batchesProcessed atomic.Int64
	bytesProcessed   atomic.Int64

	stageDurations map[string]*durationTracker
	mu             sync.RWMutex

	startTime time.Time
}

type durationTracker struct {
	total    time.Duration
	count    int64
	mu       sync.Mutex
}

func NewCollector() *Collector {
	return &Collector{
		stageDurations: make(map[string]*durationTracker),
		startTime:      time.Now(),
	}
}

func (c *Collector) RecordRead(n int64)      { c.recordsRead.Add(n) }
func (c *Collector) RecordWritten(n int64)    { c.recordsWritten.Add(n) }
func (c *Collector) RecordFailed(n int64)     { c.recordsFailed.Add(n) }
func (c *Collector) RecordFiltered(n int64)   { c.recordsFiltered.Add(n) }
func (c *Collector) BatchProcessed()          { c.batchesProcessed.Add(1) }
func (c *Collector) BytesProcessed(n int64)   { c.bytesProcessed.Add(n) }

// TrackStageDuration records how long a named stage took.
func (c *Collector) TrackStageDuration(stage string, d time.Duration) {
	c.mu.RLock()
	tracker, ok := c.stageDurations[stage]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		// Double-check after acquiring write lock
		if tracker, ok = c.stageDurations[stage]; !ok {
			tracker = &durationTracker{}
			c.stageDurations[stage] = tracker
		}
		c.mu.Unlock()
	}

	tracker.mu.Lock()
	tracker.total += d
	tracker.count++
	tracker.mu.Unlock()
}

// Snapshot represents a point-in-time view of pipeline metrics.
type Snapshot struct {
	RecordsRead      int64              `json:"records_read"`
	RecordsWritten   int64              `json:"records_written"`
	RecordsFailed    int64              `json:"records_failed"`
	RecordsFiltered  int64              `json:"records_filtered"`
	BatchesProcessed int64              `json:"batches_processed"`
	BytesProcessed   int64              `json:"bytes_processed"`
	Uptime           string             `json:"uptime"`
	Throughput       float64            `json:"records_per_second"`
	AvgStageDuration map[string]string  `json:"avg_stage_duration_ms"`
	ErrorRate        float64            `json:"error_rate_percent"`
}

// Snapshot returns a consistent view of all metrics.
func (c *Collector) Snapshot() Snapshot {
	read := c.recordsRead.Load()
	failed := c.recordsFailed.Load()
	elapsed := time.Since(c.startTime)

	var throughput float64
	if elapsed.Seconds() > 0 {
		throughput = float64(c.recordsWritten.Load()) / elapsed.Seconds()
	}

	var errorRate float64
	if read > 0 {
		errorRate = float64(failed) / float64(read) * 100
	}

	avgDurations := make(map[string]string)
	c.mu.RLock()
	for stage, tracker := range c.stageDurations {
		tracker.mu.Lock()
		if tracker.count > 0 {
			avg := tracker.total / time.Duration(tracker.count)
			avgDurations[stage] = fmt.Sprintf("%.2fms", float64(avg.Microseconds())/1000)
		}
		tracker.mu.Unlock()
	}
	c.mu.RUnlock()

	return Snapshot{
		RecordsRead:      read,
		RecordsWritten:   c.recordsWritten.Load(),
		RecordsFailed:    failed,
		RecordsFiltered:  c.recordsFiltered.Load(),
		BatchesProcessed: c.batchesProcessed.Load(),
		BytesProcessed:   c.bytesProcessed.Load(),
		Uptime:           elapsed.Round(time.Second).String(),
		Throughput:       throughput,
		AvgStageDuration: avgDurations,
		ErrorRate:        errorRate,
	}
}

// JSON returns the snapshot as formatted JSON.
func (c *Collector) JSON() (string, error) {
	snap := c.Snapshot()
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
