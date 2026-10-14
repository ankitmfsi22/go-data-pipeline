package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

func feed(n int) <-chan domain.Record {
	in := make(chan domain.Record, n)
	for i := range n {
		in <- domain.Record{Line: i + 1, Fields: map[string]any{"n": i}}
	}
	close(in)
	return in
}

func TestRunWorkers_GoodAndBad(t *testing.T) {
	in := feed(100)
	out := make(chan domain.Record, 100)
	errs := make(chan domain.RecordError, 100)
	step := func(r domain.Record) (domain.Record, error) {
		if r.Line%10 == 0 {
			return r, errors.New("bad line")
		}
		return r, nil
	}

	RunWorkers(t.Context(), "test", 4, in, out, errs, step)
	close(errs)

	good := 0
	for range out {
		good++
	}
	bad := 0
	for e := range errs {
		bad++
		if e.Stage != "test" || e.Reason != "bad line" {
			t.Errorf("unexpected error %+v", e)
		}
	}

	if good != 90 || bad != 10 {
		t.Errorf("good=%d bad=%d, want 90 and 10", good, bad)
	}
}

func TestRunWorkers_RunsInParallel(t *testing.T) {
	var running, maxRunning atomic.Int32

	step := func(r domain.Record) (domain.Record, error) {
		now := running.Add(1)
		for {
			old := maxRunning.Load()
			if now <= old || maxRunning.CompareAndSwap(old, now) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		running.Add(-1)
		return r, nil
	}

	out := make(chan domain.Record, 20)
	RunWorkers(t.Context(), "test", 5, feed(20), out, make(chan domain.RecordError, 1), step)

	if maxRunning.Load() < 2 {
		t.Errorf("max workers running at once = %d, want more than 1", maxRunning.Load())
	}
}

func TestRunWorkers_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	in := make(chan domain.Record) // never closed: workers would wait forever without ctx
	out := make(chan domain.Record)

	done := make(chan struct{})
	go func() {
		RunWorkers(ctx, "test", 3, in, out, make(chan domain.RecordError), func(r domain.Record) (domain.Record, error) {
			return r, nil
		})
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after cancel")
	}
}
