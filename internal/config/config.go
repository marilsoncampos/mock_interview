package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// PipelineConfig holds all configuration for the data pipeline.
type PipelineConfig struct {
	Sources     []SourceConfig  `json:"sources"`
	Transforms  []TransformRule `json:"transforms"`
	Destination DestConfig      `json:"destination"`
	Pipeline    PipelineOpts    `json:"pipeline"`
}

// SourceConfig defines an input data source.
type SourceConfig struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"` // "csv", "api", "database"
	Location   string            `json:"location"`
	Schedule   string            `json:"schedule"` // cron expression
	BatchSize  int               `json:"batch_size"`
	Properties map[string]string `json:"properties,omitempty"`
}

// TransformRule defines a data transformation step.
type TransformRule struct {
	Name      string                 `json:"name"`
	Operation string                 `json:"operation"` // "filter", "map", "aggregate", "join"
	Config    map[string]interface{} `json:"config"`
	DependsOn []string               `json:"depends_on,omitempty"`
}

// DestConfig defines the output destination.
type DestConfig struct {
	Type             string        `json:"type"` // "postgres", "parquet", "s3"
	ConnectionString string        `json:"connection_string"`
	TableName        string        `json:"table_name"`
	WriteMode        string        `json:"write_mode"` // "append", "upsert", "replace"
	RetryAttempts    int           `json:"retry_attempts"`
	RetryDelay       time.Duration `json:"retry_delay"`
}

// PipelineOpts defines operational pipeline settings.
type PipelineOpts struct {
	Workers        int           `json:"workers"`
	BufferSize     int           `json:"buffer_size"`
	FlushInterval  time.Duration `json:"flush_interval"`
	MaxRetries     int           `json:"max_retries"`
	DeadLetterPath string        `json:"dead_letter_path"`
	EnableMetrics  bool          `json:"enable_metrics"`
}

// Load reads and validates a pipeline configuration from a JSON file.
func Load(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg PipelineConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func (c *PipelineConfig) validate() error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	if c.Pipeline.Workers <= 0 {
		c.Pipeline.Workers = 4 // sensible default
	}
	if c.Pipeline.BufferSize <= 0 {
		c.Pipeline.BufferSize = 1000
	}
	for _, src := range c.Sources {
		if src.Name == "" || src.Type == "" {
			return fmt.Errorf("source name and type are required")
		}
		if src.BatchSize <= 0 {
			return fmt.Errorf("source %q: batch_size must be positive", src.Name)
		}
	}
	return nil
}
