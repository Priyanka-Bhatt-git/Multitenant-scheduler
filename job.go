package scheduler

import "time"

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusRetrying  JobStatus = "retrying"
	StatusEscalated JobStatus = "escalated"
)

// Job represents a single unit of work submitted by a tenant.
type Job struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	Priority   int       `json:"priority"` // higher value = higher priority
	Payload    string    `json:"payload"`
	Status     JobStatus `json:"status"`
	Attempts   int       `json:"attempts"`
	MaxRetries int       `json:"max_retries"`
	CreatedAt  time.Time `json:"created_at"`
	NextRunAt  time.Time `json:"next_run_at"`
	LastError  string    `json:"last_error,omitempty"`
}
