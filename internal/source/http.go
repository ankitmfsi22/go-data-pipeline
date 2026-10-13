package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

const (
	defaultHTTPTimeout = 30 * time.Second
	maxBodyBytes       = 50 << 20
)

type HTTP struct {
	URL     string
	Timeout time.Duration
	Client  *http.Client
}

func (h *HTTP) Name() string { return h.URL }
func (h *HTTP) Read(ctx context.Context, out chan<- domain.Record) error {
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, http.NoBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes))
	dec.UseNumber()

	var body any
	if err := dec.Decode(&body); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	switch v := body.(type) {
	case []any:
		for i, item := range v {
			obj, ok := item.(map[string]any)
			if !ok {
				return fmt.Errorf("item %d is not a JSON object", i+1)
			}
			if err := send(ctx, out, domain.Record{Source: h.Name(), Line: i + 1, Fields: obj}); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		return send(ctx, out, domain.Record{Source: h.Name(), Line: 1, Fields: v})
	default:
		return fmt.Errorf("response must be a JSON object or array, got %T", body)
	}
}
