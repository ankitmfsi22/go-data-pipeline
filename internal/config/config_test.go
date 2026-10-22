package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with no env vars returned error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.DefaultWorkers != 4 {
		t.Errorf("DefaultWorkers = %d, want 4", cfg.DefaultWorkers)
	}
	if cfg.ShutdownTimeout != 20*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 20s", cfg.ShutdownTimeout)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("PIPELINE_DEFAULT_WORKERS", "16")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.DefaultWorkers != 16 {
		t.Errorf("DefaultWorkers = %d, want 16", cfg.DefaultWorkers)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 5s", cfg.ShutdownTimeout)
	}
}

func TestLoad_InvalidValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"non-integer workers", "PIPELINE_DEFAULT_WORKERS", "abc"},
		{"zero workers", "PIPELINE_DEFAULT_WORKERS", "0"},
		{"bad duration", "SHUTDOWN_TIMEOUT", "ten seconds"},
		{"unknown log level", "LOG_LEVEL", "verbose"},
		{"unknown log format", "LOG_FORMAT", "xml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.key, tt.val)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() with %s=%q expected error, got nil", tt.key, tt.val)
			}
		})
	}
}
