package job

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/metrics"
	"github.com/ankitmfsi22/go-data-pipeline/internal/pipeline"
	"github.com/ankitmfsi22/go-data-pipeline/internal/store"
)

var (
	ErrNotFound    = store.ErrNotFound
	ErrInvalidSpec = errors.New("invalid spec")
	ErrFinished    = errors.New("job already finished")
	ErrNotFinished = errors.New("job is still running")
)

type Config struct {
	Workers    int
	BufferSize int
	Timeout    time.Duration
	MaxRunning int
	Metrics    *metrics.Metrics
}
type Manager struct {
	store  *store.SQLite
	log    *slog.Logger
	client *http.Client
	cfg    Config

	slots chan struct{}
	wg    sync.WaitGroup

	mu   sync.Mutex
	runs map[string]*run
}

type run struct {
	cancel   context.CancelFunc
	progress *Tracker
}

func NewManager(ctx context.Context, st *store.SQLite, log *slog.Logger, cfg Config) (*Manager, error) {
	if cfg.MaxRunning < 1 {
		cfg.MaxRunning = 1
	}
	n, err := st.FailUnfinished(ctx)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		log.Warn("marked unfinished jobs as failed", "count", n)
	}
	return &Manager{
		store:  st,
		log:    log,
		client: &http.Client{},
		cfg:    cfg,
		slots:  make(chan struct{}, cfg.MaxRunning),
		runs:   map[string]*run{},
	}, nil
}
func (m *Manager) Start(ctx context.Context, spec domain.Spec) (domain.Job, error) {
	spec.ApplyDefaults(m.cfg.Workers, m.cfg.BufferSize, m.cfg.Timeout)
	if err := spec.Validate(); err != nil {
		return domain.Job{}, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}
	plan, err := pipeline.NewPlan(spec, pipeline.Deps{HTTPClient: m.client, Saver: m.store})
	if err != nil {
		return domain.Job{}, fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}

	j := domain.Job{
		ID:        newID(),
		Name:      spec.Name,
		Status:    domain.StatusPending,
		Spec:      spec,
		CreatedAt: time.Now().UTC(),
	}
	if err := m.store.CreateJob(ctx, j); err != nil {
		return domain.Job{}, err
	}

	jobCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r := &run{cancel: cancel, progress: NewTracker()}

	m.mu.Lock()
	m.runs[j.ID] = r
	m.mu.Unlock()
	if m.cfg.Metrics != nil {
		m.cfg.Metrics.JobsCreated.Inc()
	}
	m.wg.Add(1)
	go m.execute(jobCtx, j, plan, r)

	m.log.Info("job created", "id", j.ID, "name", j.Name)
	return j, nil
}

func (m *Manager) execute(ctx context.Context, j domain.Job, plan *pipeline.Plan, r *run) {
	defer m.wg.Done()
	defer r.cancel()
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		m.finish(ctx, j, r, ctx.Err(), nil)
		return
	}
	runCtx, stop := context.WithTimeout(ctx, j.Spec.Options.Timeout.Std())
	defer stop()

	now := time.Now().UTC()
	j.Status, j.StartedAt = domain.StatusRunning, &now
	m.save(runCtx, j)
	r.progress.Start()
	m.log.Info("job started", "id", j.ID)

	var obs pipeline.Observer = r.progress
	if met := m.cfg.Metrics; met != nil {
		met.JobsRunning.Inc()
		defer met.JobsRunning.Dec()
		obs = observers{r.progress, met}
	}
	out, err := plan.Run(runCtx, j.ID, obs)
	m.finish(runCtx, j, r, err, out.Errors)
}

func (m *Manager) finish(ctx context.Context, j domain.Job, r *run, err error, rejected []domain.RecordError) {
	r.progress.Stop()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if len(rejected) > 0 {
		if err := m.store.SaveErrors(ctx, j.ID, rejected); err != nil {
			m.log.Error("save job errors", "id", j.ID, "err", err)
		}
	}

	now := time.Now().UTC()
	j.FinishedAt = &now
	switch {
	case err == nil:
		j.Status = domain.StatusCompleted
	case errors.Is(err, context.Canceled):
		j.Status, j.Error = domain.StatusCancelled, "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		j.Status, j.Error = domain.StatusFailed, "timed out after "+j.Spec.Options.Timeout.Std().String()
	default:
		j.Status, j.Error = domain.StatusFailed, err.Error()
	}

	if err := m.store.UpdateJob(ctx, j); err != nil {
		m.log.Error("save job", "id", j.ID, "err", err)
	}
	if m.cfg.Metrics != nil {
		m.cfg.Metrics.JobsFinished.Inc(string(j.Status))
	}
	p := r.progress.Snapshot()
	m.log.Info("job finished", "id", j.ID, "status", j.Status,
		"read", p.RecordsRead, "passed", p.RecordsPassed, "failed", p.RecordsFailed, "elapsed", p.Elapsed.Std())
}

func (m *Manager) save(ctx context.Context, j domain.Job) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := m.store.UpdateJob(ctx, j); err != nil {
		m.log.Error("save job", "id", j.ID, "err", err)
	}
}
func (m *Manager) Get(ctx context.Context, id string) (domain.Job, error) {
	return m.store.GetJob(ctx, id)
}
func (m *Manager) List(ctx context.Context) ([]domain.Job, error) {
	return m.store.ListJobs(ctx)
}
func (m *Manager) Progress(id string) (domain.Progress, bool) {
	m.mu.Lock()
	r, ok := m.runs[id]
	m.mu.Unlock()
	if !ok {
		return domain.Progress{}, false
	}
	return r.progress.Snapshot(), true
}

func (m *Manager) Cancel(ctx context.Context, id string) error {
	j, err := m.store.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if j.Status.Done() {
		return ErrFinished
	}

	m.mu.Lock()
	r, ok := m.runs[id]
	m.mu.Unlock()
	if !ok {
		return ErrFinished
	}
	r.cancel()
	m.log.Info("job cancel requested", "id", id)
	return nil
}
func (m *Manager) Delete(ctx context.Context, id string) error {
	j, err := m.store.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !j.Status.Done() {
		return ErrNotFinished
	}
	if err := m.store.DeleteJob(ctx, id); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.runs, id)
	m.mu.Unlock()
	return nil
}
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	for _, r := range m.runs {
		r.cancel()
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type observers []pipeline.Observer

func (o observers) Observe(stage string, took time.Duration, failed bool) {
	for _, ob := range o {
		ob.Observe(stage, took, failed)
	}
}
func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "job_" + hex.EncodeToString(b)
}

type Results struct {
	JobID        string             `json:"job_id"`
	Status       domain.Status      `json:"status"`
	Summary      []domain.AggResult `json:"summary"`
	TotalRecords int                `json:"total_records"`
	Records      []domain.Record    `json:"records"`
}

func (m *Manager) Results(ctx context.Context, id string, limit, offset int) (Results, error) {
	j, err := m.store.GetJob(ctx, id)
	if err != nil {
		return Results{}, err
	}
	if !j.Status.Done() {
		return Results{}, ErrNotFinished
	}

	res := Results{JobID: id, Status: j.Status}
	if res.Summary, err = m.store.GetSummary(ctx, id); err != nil {
		return Results{}, err
	}
	if res.TotalRecords, err = m.store.CountRecords(ctx, id); err != nil {
		return Results{}, err
	}
	if res.Records, err = m.store.ListRecords(ctx, id, limit, offset); err != nil {
		return Results{}, err
	}
	return res, nil
}
func (m *Manager) Errors(ctx context.Context, id string) ([]domain.RecordError, error) {
	if _, err := m.store.GetJob(ctx, id); err != nil {
		return nil, err
	}
	return m.store.ListErrors(ctx, id)
}
