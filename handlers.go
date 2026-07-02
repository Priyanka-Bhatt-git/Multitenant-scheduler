package api

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"multitenant-scheduler/scheduler"
)

// Server wires the scheduler up to HTTP handlers.
type Server struct {
	Scheduler *scheduler.Scheduler
	seq       int64
}

type submitRequest struct {
	TenantID   string `json:"tenant_id"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
}

// SubmitJob handles POST /jobs/submit
func (s *Server) SubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.TenantID == "" {
		http.Error(w, "tenant_id is required", http.StatusBadRequest)
		return
	}

	seq := atomic.AddInt64(&s.seq, 1)
	job := &scheduler.Job{
		ID:         scheduler.GenerateJobID(req.TenantID, int(seq)),
		TenantID:   req.TenantID,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
	}
	s.Scheduler.Submit(job)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// GetJob handles GET /jobs/get?id=<job_id>
func (s *Server) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	job, ok := s.Scheduler.GetJob(id)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// ListTenantJobs handles GET /tenants/jobs?tenant_id=<tenant_id>
func (s *Server) ListTenantJobs(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	if tenantID == "" {
		http.Error(w, "tenant_id query param is required", http.StatusBadRequest)
		return
	}
	jobs := s.Scheduler.ListTenantJobs(tenantID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}
