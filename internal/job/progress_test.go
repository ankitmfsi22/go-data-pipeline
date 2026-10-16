package job

import (
	"sync"
	"testing"
	"time"
)

func TestTracker_NotStarted(t *testing.T) {
	p := NewTracker().Snapshot()
	if p.RecordsRead != 0 || p.Elapsed != 0 {
		t.Errorf("got %+v, want zeros", p)
	}
}

func TestTracker_Counts(t *testing.T) {
	tr := NewTracker()
	tr.Start()
	for i := range 10 {
		tr.Observe("validate", time.Millisecond, i < 2)
	}
	for i := range 8 {
		tr.Observe("transform", 2*time.Millisecond, i == 0)
	}
	tr.Observe("unknown", time.Second, true)
	tr.Stop()

	p := tr.Snapshot()
	if p.RecordsRead != 10 || p.RecordsPassed != 7 || p.RecordsFailed != 3 {
		t.Errorf("read/passed/failed = %d/%d/%d, want 10/7/3", p.RecordsRead, p.RecordsPassed, p.RecordsFailed)
	}
	if p.FailedByStage["validate"] != 2 || p.FailedByStage["transform"] != 1 {
		t.Errorf("failed by stage = %v", p.FailedByStage)
	}
	if p.StageLatencyMs["validate"] != 1 || p.StageLatencyMs["transform"] != 2 {
		t.Errorf("latency = %v, want validate 1ms, transform 2ms", p.StageLatencyMs)
	}
}

func TestTracker_StopFreezesElapsed(t *testing.T) {
	tr := NewTracker()
	tr.Start()
	tr.Stop()
	first := tr.Snapshot().Elapsed
	time.Sleep(5 * time.Millisecond)
	if got := tr.Snapshot().Elapsed; got != first {
		t.Errorf("elapsed changed after Stop: %v -> %v", first, got)
	}
}

func TestTracker_Concurrent(t *testing.T) {
	tr := NewTracker()
	tr.Start()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				tr.Observe("validate", time.Microsecond, false)
				_ = tr.Snapshot()
			}
		}()
	}
	wg.Wait()

	if got := tr.Snapshot().RecordsRead; got != 5000 {
		t.Errorf("records read = %d, want 5000", got)
	}
}
