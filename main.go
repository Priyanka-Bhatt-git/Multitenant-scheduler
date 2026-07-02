package main

import (
	"log"
	"net/http"
	"time"

	"multitenant-scheduler/api"
	"multitenant-scheduler/scheduler"
)

func main() {
	// Replace this with real task execution (e.g. calling another service,
	// running a batch operation, sending a notification, etc).
	handler := func(job *scheduler.Job) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	sched := scheduler.NewScheduler(4, handler)

	// Default rate limit for all tenants:
	// 5 job submissions / second, with burst up to 10.
	sched.SetDefaultRateLimit(5, 10)

	// Optional tenant-specific override example:
	// premium-tenant can submit 20 jobs/sec with burst 40.
	sched.SetTenantRateLimit("premium-tenant", 20, 40)

	sched.Start()
	defer sched.Stop()

	server := &api.Server{Scheduler: sched}

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/submit", server.SubmitJob)
	mux.HandleFunc("/jobs/get", server.GetJob)
	mux.HandleFunc("/tenants/jobs", server.ListTenantJobs)

	log.Println("multi-tenant scheduler listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
