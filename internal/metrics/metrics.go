package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct{ n atomic.Int64 }

func (c *Counter) Inc()         { c.n.Add(1) }
func (c *Counter) Value() int64 { return c.n.Load() }

type Gauge struct{ n atomic.Int64 }

func (g *Gauge) Inc() { g.n.Add(1) }

func (g *Gauge) Dec() { g.n.Add(-1) }

func (g *Gauge) Value() int64 { return g.n.Load() }

type series struct {
	labels []string
	count  atomic.Int64
	sum    atomic.Uint64
}

func (s *series) addSum(v float64) {
	for {
		old := s.sum.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if s.sum.CompareAndSwap(old, next) {
			return
		}
	}
}

type vec struct {
	names []string
	mu    sync.Mutex
	all   map[string]*series
}

func newVec(names ...string) *vec {
	return &vec{names: names, all: map[string]*series{}}
}

func (v *vec) get(values ...string) *series {
	key := strings.Join(values, "\x00")
	v.mu.Lock()
	defer v.mu.Unlock()
	s, ok := v.all[key]
	if !ok {
		s = &series{labels: values}
		v.all[key] = s
	}
	return s
}
func (v *vec) sorted() []*series {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]*series, 0, len(v.all))
	for _, s := range v.all {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b *series) int {
		return strings.Compare(strings.Join(a.labels, ","), strings.Join(b.labels, ","))
	})
	return out
}

type CounterVec struct{ *vec }

func (c CounterVec) Inc(values ...string)         { c.get(values...).count.Add(1) }
func (c CounterVec) Value(values ...string) int64 { return c.get(values...).count.Load() }

type SummaryVec struct{ *vec }

func (s SummaryVec) Observe(v float64, values ...string) {
	sr := s.get(values...)
	sr.addSum(v)
	sr.count.Add(1)
}

type Metrics struct {
	JobsCreated   Counter
	JobsRunning   Gauge
	JobsFinished  CounterVec
	Records       CounterVec
	RecordsFailed CounterVec
	StageSeconds  SummaryVec
	HTTPRequests  CounterVec
	HTTPSeconds   SummaryVec
}

func New() *Metrics {
	return &Metrics{
		JobsFinished:  CounterVec{newVec("status")},
		Records:       CounterVec{newVec("stage")},
		RecordsFailed: CounterVec{newVec("stage")},
		StageSeconds:  SummaryVec{newVec("stage")},
		HTTPRequests:  CounterVec{newVec("route", "status")},
		HTTPSeconds:   SummaryVec{newVec("route")},
	}
}
func (m *Metrics) Observe(stage string, took time.Duration, failed bool) {
	m.Records.Inc(stage)
	m.StageSeconds.Observe(took.Seconds(), stage)
	if failed {
		m.RecordsFailed.Inc(stage)
	}
}
func (m *Metrics) WriteTo(w io.Writer) (int64, error) {
	var b strings.Builder

	single(&b, "pipeline_jobs_created_total", "counter", "Jobs accepted by the API.", m.JobsCreated.Value())
	single(&b, "pipeline_jobs_running", "gauge", "Jobs running right now.", m.JobsRunning.Value())
	counters(&b, "pipeline_jobs_finished_total", "Jobs finished, by final status.", m.JobsFinished)
	counters(&b, "pipeline_records_total", "Records handled, by stage.", m.Records)
	counters(&b, "pipeline_records_failed_total", "Records rejected, by stage.", m.RecordsFailed)
	summary(&b, "pipeline_stage_duration_seconds", "Time spent per record, by stage.", m.StageSeconds)
	counters(&b, "http_requests_total", "HTTP requests, by route and status.", m.HTTPRequests)
	summary(&b, "http_request_duration_seconds", "HTTP request time, by route.", m.HTTPSeconds)
	single(&b, "go_goroutines", "gauge", "Goroutines that currently exist.", int64(runtime.NumGoroutine()))

	n, err := io.WriteString(w, b.String())
	return int64(n), err
}
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = m.WriteTo(w)
	})
}

func header(b *strings.Builder, name, kind, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

func single(b *strings.Builder, name, kind, help string, v int64) {
	header(b, name, kind, help)
	fmt.Fprintf(b, "%s %d\n", name, v)
}

func counters(b *strings.Builder, name, help string, c CounterVec) {
	header(b, name, "counter", help)
	for _, s := range c.sorted() {
		fmt.Fprintf(b, "%s%s %d\n", name, labels(c.names, s.labels), s.count.Load())
	}
}

func summary(b *strings.Builder, name, help string, sv SummaryVec) {
	header(b, name, "summary", help)
	for _, s := range sv.sorted() {
		l := labels(sv.names, s.labels)
		fmt.Fprintf(b, "%s_sum%s %g\n", name, l, math.Float64frombits(s.sum.Load()))
		fmt.Fprintf(b, "%s_count%s %d\n", name, l, s.count.Load())
	}
}
func labels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = fmt.Sprintf(`%s="%s"`, n, r.Replace(values[i]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
