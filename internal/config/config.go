// Package config loads application settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime settings for the service.
type Config struct {
	HTTPAddr          string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          string
	LogFormat         string
	DatabaseDriver    string
	DatabaseURL       string
	OutputDir         string
	DefaultWorkers    int
	DefaultBufferSize int
	DefaultJobTimeout time.Duration
	MaxConcurrentJobs int
}

// Load reads configuration from the environment and validates it.
func Load() (Config, error) {
	var errs []error

	cfg := Config{
		HTTPAddr:          getString("HTTP_ADDR", ":8080"),
		ReadHeaderTimeout: getDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second, &errs),
		ReadTimeout:       getDuration("HTTP_READ_TIMEOUT", 15*time.Second, &errs),
		WriteTimeout:      getDuration("HTTP_WRITE_TIMEOUT", 30*time.Second, &errs),
		IdleTimeout:       getDuration("HTTP_IDLE_TIMEOUT", 60*time.Second, &errs),
		ShutdownTimeout:   getDuration("SHUTDOWN_TIMEOUT", 20*time.Second, &errs),
		LogLevel:          getString("LOG_LEVEL", "info"),
		LogFormat:         getString("LOG_FORMAT", "json"),
		DatabaseDriver:    getString("DB_DRIVER", "sqlite"),
		DatabaseURL:       getString("DATABASE_URL", "pipeline.db"),
		OutputDir:         getString("OUTPUT_DIR", "./output"),
		DefaultWorkers:    getInt("PIPELINE_DEFAULT_WORKERS", 4, &errs),
		DefaultBufferSize: getInt("PIPELINE_DEFAULT_BUFFER", 100, &errs),
		DefaultJobTimeout: getDuration("PIPELINE_DEFAULT_TIMEOUT", 10*time.Minute, &errs),
		MaxConcurrentJobs: getInt("PIPELINE_MAX_CONCURRENT_JOBS", 5, &errs),
	}

	errs = append(errs, cfg.validate()...)
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func (c Config) validate() []error {
	var errs []error
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("LOG_LEVEL must be debug|info|warn|error, got %q", c.LogLevel))
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json|text, got %q", c.LogFormat))
	}
	switch c.DatabaseDriver {
	case "sqlite", "postgres":
	default:
		errs = append(errs, fmt.Errorf("DB_DRIVER must be sqlite|postgres, got %q", c.DatabaseDriver))
	}
	if c.DefaultWorkers < 1 {
		errs = append(errs, errors.New("PIPELINE_DEFAULT_WORKERS must be >= 1"))
	}
	if c.DefaultBufferSize < 0 {
		errs = append(errs, errors.New("PIPELINE_DEFAULT_BUFFER must be >= 0"))
	}
	if c.MaxConcurrentJobs < 1 {
		errs = append(errs, errors.New("PIPELINE_MAX_CONCURRENT_JOBS must be >= 1"))
	}
	return errs
}

func getString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func getDuration(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a duration (e.g. 30s, 5m)", key, v))
		return def
	}
	return d
}
