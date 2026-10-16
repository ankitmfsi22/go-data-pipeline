package job

import (
	"sync/atomic"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
)

var trackedStages = []string{"validate", "transform"}

type stageStats struct {
	count  atomic.Int64
	failed atomic.Int64
	nanos  atomic.Int64
}

type Tracker struct {
	started  atomic.Int64
	finished atomic.Int64
	stages   map[string]*stageStats
}

func NewTracker() *Tracker {
	t := &Tracker{stages: make(map[string]*stageStats, len(trackedStages))}
	for _, name := range trackedStages {
		t.stages[name] = &stageStats{}
	}
	return t
}

func (t *Tracker) Start() { t.started.Store(time.Now().UnixNano()) }
func (t *Tracker) Stop()  { t.finished.CompareAndSwap(0, time.Now().UnixNano()) }
func (t *Tracker) Observe(stage string, took time.Duration, failed bool) {
	s, ok := t.stages[stage]
	if !ok {
		return
	}
	s.count.Add(1)
	s.nanos.Add(int64(took))
	if failed {
		s.failed.Add(1)
	}
}

func (t *Tracker) Snapshot() domain.Progress {
	p := domain.Progress{
		FailedByStage:  map[string]int64{},
		StageLatencyMs: map[string]float64{},
	}

	start := t.started.Load()
	if start == 0 {
		return p
	}
	end := t.finished.Load()
	if end == 0 {
		end = time.Now().UnixNano()
	}
	elapsed := time.Duration(end - start)

	for name, s := range t.stages {
		n, f := s.count.Load(), s.failed.Load()
		p.RecordsFailed += f
		p.FailedByStage[name] = f
		if n > 0 {
			p.StageLatencyMs[name] = float64(s.nanos.Load()) / float64(n) / 1e6
		}
	}

	p.RecordsRead = t.stages["validate"].count.Load()
	p.RecordsPassed = t.stages["transform"].count.Load() - t.stages["transform"].failed.Load()

	if sec := elapsed.Seconds(); sec > 0 {
		p.RecordsPerSec = float64(p.RecordsRead) / sec
	}
	p.Elapsed = domain.Duration(elapsed)
	return p
}
