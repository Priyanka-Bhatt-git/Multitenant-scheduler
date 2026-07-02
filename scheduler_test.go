package scheduler

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSubmitAndRunSucceeds(t *testing.T) {
	var mu sync.Mutex
	ran := make(map[string]bool)

	handler := func(job *Job) error {
		mu.Lock()
		ran[job.ID] = true
		mu.Unlock()
		return nil
	}

	s := NewScheduler(2, handler)
	s.Start()
	defer s.Stop()

	job := &Job{ID: "t1-job-1", TenantID: "t1", Priority: 1, MaxRetries: 2}
	s.Submit(job)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !ran["t1-job-1"] {
		t.Fatalf("expected job t1-job-1 to have run")
	}

	got, ok := s.GetJob("t1-job-1")
	if !ok || got.Status != StatusSucceeded {
		t.Fatalf("expected job status succeeded, got %+v", got)
	}
}

func TestRetryOnFailure(t *testing.T) {
	var attempts int
	var mu sync.Mutex

	handler := func(job *Job) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return errors.New("simulated failure")
	}

	s := NewScheduler(1, handler)
	s.Start()
	defer s.Stop()

	job := &Job{ID: "t1-job-2", TenantID: "t1", Priority: 1, MaxRetries: 2}
	s.Submit(job)

	time.Sleep(1 * time.Second)

	got, ok := s.GetJob("t1-job-2")
	if !ok {
		t.Fatalf("job not found")
	}
	if got.Status != StatusFailed {
		t.Fatalf("expected job status failed after exceeding retries, got %s", got.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 { // initial attempt + 2 retries
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

// fakeDecider lets tests control the agentic decision without calling a
// real LLM API.
type fakeDecider struct {
	action RetryAction
}

func (f *fakeDecider) Decide(job *Job) RetryAction {
	return f.action
}

func TestAgentSkipStopsRetryingEarly(t *testing.T) {
	handler := func(job *Job) error {
		return errors.New("validation error: malformed payload")
	}

	s := NewScheduler(1, handler)
	s.SetDecider(&fakeDecider{action: ActionSkip})
	s.Start()
	defer s.Stop()

	// MaxRetries is high, but the agent should skip well before that,
	// since it decides the error is permanent.
	job := &Job{ID: "t1-job-skip", TenantID: "t1", Priority: 1, MaxRetries: 10}
	s.Submit(job)

	time.Sleep(200 * time.Millisecond)

	got, ok := s.GetJob("t1-job-skip")
	if !ok {
		t.Fatalf("job not found")
	}
	if got.Status != StatusFailed {
		t.Fatalf("expected job to be failed (skipped) by agent, got %s", got.Status)
	}
	if got.Attempts != 1 {
		t.Fatalf("expected only 1 attempt before agent skip, got %d", got.Attempts)
	}
}

func TestAgentEscalate(t *testing.T) {
	handler := func(job *Job) error {
		return errors.New("unexpected downstream state")
	}

	s := NewScheduler(1, handler)
	s.SetDecider(&fakeDecider{action: ActionEscalate})
	s.Start()
	defer s.Stop()

	job := &Job{ID: "t1-job-escalate", TenantID: "t1", Priority: 1, MaxRetries: 5}
	s.Submit(job)

	time.Sleep(200 * time.Millisecond)

	got, ok := s.GetJob("t1-job-escalate")
	if !ok {
		t.Fatalf("job not found")
	}
	if got.Status != StatusEscalated {
		t.Fatalf("expected job to be escalated by agent, got %s", got.Status)
	}
}

func TestTenantFairness(t *testing.T) {
	var mu sync.Mutex
	order := []string{}

	handler := func(job *Job) error {
		mu.Lock()
		order = append(order, job.TenantID)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	s := NewScheduler(1, handler)
	s.Start()
	defer s.Stop()

	// Tenant A submits 5 jobs, tenant B submits 1 job. Round-robin should
	// interleave B's job early instead of letting A starve it.
	for i := 0; i < 5; i++ {
		s.Submit(&Job{ID: GenerateJobID("A", i), TenantID: "A", Priority: 1})
	}
	s.Submit(&Job{ID: GenerateJobID("B", 0), TenantID: "B", Priority: 1})

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	bIndex := -1
	for i, tid := range order {
		if tid == "B" {
			bIndex = i
			break
		}
	}
	if bIndex == -1 {
		t.Fatalf("tenant B job never ran")
	}
	if bIndex > 1 {
		t.Fatalf("expected tenant B to run early due to round-robin fairness, ran at index %d: %v", bIndex, order)
	}
}
