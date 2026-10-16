package domain

import "time"

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func (s Status) Done() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

type Job struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Status     Status     `json:"status"`
	Error      string     `json:"error,omitempty"`
	Spec       Spec       `json:"spec"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type Progress struct {
	RecordsRead    int64              `json:"records_read"`
	RecordsPassed  int64              `json:"records_passed"`
	RecordsFailed  int64              `json:"records_failed"`
	RecordsPerSec  float64            `json:"records_per_sec"`
	Elapsed        Duration           `json:"elapsed"`
	FailedByStage  map[string]int64   `json:"failed_by_stage"`
	StageLatencyMs map[string]float64 `json:"stage_latency_ms"`
}
