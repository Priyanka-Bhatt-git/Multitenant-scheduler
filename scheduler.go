package scheduler

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// JobHandler executes the actual work for a job. Return an error to trigger
// a retry (subject to MaxRetries).
type JobHandler func(job *Job) error

// Scheduler coordinates job execution across multiple tenants, guaranteeing
// that no single tenant can starve the others (round-robin fairness), and
// retries failed jobs with exponential backoff.
type Scheduler struct {
	mu           sync.Mutex
	tenantQueues map[string]*TenantQueue
	tenantOrder  []string
	rrIndex      int
	jobIndex     map[string]*Job

	handler JobHandler
	workers int
	stopCh  chan struct{}
	wg      sync.WaitGroup

	retryAgent Decider
}

func NewScheduler(workers int, handler JobHandler) *Scheduler {
	return &Scheduler{
		tenantQueues: make(map[string]*TenantQueue),
		jobIndex:     make(map[string]*Job),
		handler:      handler,
		workers:      workers,
		stopCh:       make(chan struct{}),
		retryAgent:   NewRetryAgent(),
	}
}

// SetDecider overrides the retry-decision logic. Useful for tests, or to
// swap in a different policy without touching the scheduler internals.
func (s *Scheduler) SetDecider(d Decider) {
	s.retryAgent = d
}

// Submit enqueues a job under its tenant's isolated queue.
func (s *Scheduler) Submit(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	if job.Status == "" {
		job.Status = StatusPending
	}
	if job.NextRunAt.IsZero() {
		job.NextRunAt = time.Now()
	}

	tq, ok := s.tenantQueues[job.TenantID]
	if !ok {
		tq = NewTenantQueue()
		s.tenantQueues[job.TenantID] = tq
		s.tenantOrder = append(s.tenantOrder, job.TenantID)
	}
	tq.Push(job)
	s.jobIndex[job.ID] = job
}

func (s *Scheduler) GetJob(id string) (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobIndex[id]
	return j, ok
}

func (s *Scheduler) ListTenantJobs(tenantID string) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var jobs []*Job
	for _, j := range s.jobIndex {
		if j.TenantID == tenantID {
			jobs = append(jobs, j)
		}
	}
	return jobs
}

// nextJob picks the next runnable job using round-robin across tenants so
// that a tenant with many queued jobs cannot starve a tenant with few.
func (s *Scheduler) nextJob() *Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(s.tenantOrder)
	if n == 0 {
		return nil
	}

	now := time.Now()
	for i := 0; i < n; i++ {
		idx := (s.rrIndex + i) % n
		tenantID := s.tenantOrder[idx]
		tq := s.tenantQueues[tenantID]
		if tq.Len() == 0 {
			continue
		}
		job := tq.Pop()
		if job.NextRunAt.After(now) {
			// Job is in its backoff window; put it back and check the
			// next tenant instead of blocking on it.
			tq.Push(job)
			continue
		}
		s.rrIndex = (idx + 1) % n
		return job
	}
	return nil
}

func (s *Scheduler) Start() {
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Scheduler) worker() {
	defer s.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			job := s.nextJob()
			if job == nil {
				continue
			}
			s.runJob(job)
		}
	}
}

func (s *Scheduler) runJob(job *Job) {
	job.Status = StatusRunning
	job.Attempts++

	err := s.handler(job)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err == nil {
		job.Status = StatusSucceeded
		return
	}

	job.LastError = err.Error()

	// Hard stop regardless of the agent's opinion once the retry budget
	// is fully exhausted.
	if job.Attempts > job.MaxRetries {
		job.Status = StatusFailed
		return
	}

	action := s.retryAgent.Decide(job)
	switch action {
	case ActionSkip:
		// Agent determined the error is permanent (e.g. validation error) -
		// retrying more times would never succeed, so stop early even
		// though retry budget remains.
		job.Status = StatusFailed
		job.LastError = job.LastError + " (skipped by retry agent: non-retryable)"
		return
	case ActionEscalate:
		// Agent determined this needs human judgement rather than an
		// automatic retry or automatic give-up.
		job.Status = StatusEscalated
		return
	default: // ActionRetry
		backoff := time.Duration(math.Pow(2, float64(job.Attempts))) * 100 * time.Millisecond
		jitter := time.Duration(rand.Intn(50)) * time.Millisecond
		job.NextRunAt = time.Now().Add(backoff + jitter)
		job.Status = StatusRetrying

		tq := s.tenantQueues[job.TenantID]
		tq.Push(job)
	}
}

// GenerateJobID builds a reasonably unique job ID from tenant, sequence
// number, and current time.
func GenerateJobID(tenantID string, seq int) string {
	return fmt.Sprintf("%s-job-%d-%d", tenantID, seq, time.Now().UnixNano())
}
