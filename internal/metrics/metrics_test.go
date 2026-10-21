package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetrics_Output(t *testing.T) {
	m := New()
	m.JobsCreated.Inc()
	m.JobsRunning.Inc()
	m.JobsFinished.Inc("completed")
	m.Observe("validate", 2*time.Millisecond, false)
	m.Observe("validate", 4*time.Millisecond, true)
	m.HTTPRequests.Inc("GET /api/v1/pipelines", "200")

	var b strings.Builder
	if _, err := m.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()

	for _, want := range []string{
		"# TYPE pipeline_jobs_created_total counter",
		"pipeline_jobs_created_total 1",
		"pipeline_jobs_running 1",
		`pipeline_jobs_finished_total{status="completed"} 1`,
		`pipeline_records_total{stage="validate"} 2`,
		`pipeline_records_failed_total{stage="validate"} 1`,
		`pipeline_stage_duration_seconds_sum{stage="validate"} 0.006`,
		`pipeline_stage_duration_seconds_count{stage="validate"} 2`,
		`http_requests_total{route="GET /api/v1/pipelines",status="200"} 1`,
		"# TYPE go_goroutines gauge",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q\n%s", want, out)
		}
	}
}

func TestMetrics_LabelEscaping(t *testing.T) {
	if got := labels([]string{"route"}, []string{`a"b\c`}); got != `{route="a\"b\\c"}` {
		t.Errorf("labels = %s", got)
	}
}
func TestMetrics_Concurrent(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				m.Observe("transform", time.Microsecond, false)
				_, _ = m.WriteTo(&strings.Builder{})
			}
		}()
	}
	wg.Wait()
	if got := m.Records.Value("transform"); got != 2000 {
		t.Errorf("records = %d, want 2000", got)
	}
}

func TestMetrics_Handler(t *testing.T) {
	rec := httptest.NewRecorder()
	New().Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Errorf("code %d, content type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
}
