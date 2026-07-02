# Multi-Tenant Job Scheduler

A background job scheduler in Go that isolates work per tenant, enforces fairness across tenants using round-robin scheduling, rate-limits tenant submissions using token buckets, and retries failed jobs with exponential backoff and jitter.

## Why this exists

In a shared system, a single noisy tenant can overwhelm a job queue by continuously submitting work, delaying or starving smaller tenants. Many simple schedulers use one global FIFO or priority queue, which makes this problem worse.

This project models a fairer multi-tenant scheduler by combining **tenant isolation**, **fair dispatch**, and **tenant-level workload control**:

* each tenant gets its own isolated priority queue
* jobs are dispatched across tenants in round-robin order
* tenants are rate-limited at submission time using token buckets
* failed jobs are retried with exponential backoff instead of immediately re-entering the queue

The goal is to show how a shared scheduler can protect itself from both **queue starvation** and **submission floods** in a multi-tenant environment.

## What it does

* **Per-tenant queue isolation** — jobs are grouped by tenant instead of being pushed into one shared global queue
* **Fair scheduling across tenants** — tenants are scheduled in round-robin order so smaller tenants are not starved behind a large backlog from another tenant
* **Priority within a tenant** — each tenant queue is a priority queue, so more important jobs for that tenant are processed first
* **Per-tenant token-bucket rate limiting** — each tenant can only submit jobs at a configured rate with a configurable burst capacity, preventing a noisy tenant from flooding the scheduler
* **Retry with backoff** — failed jobs are retried with exponential backoff and jitter to avoid immediate retry storms
* **Pluggable retry decisioning** — a retry agent can classify failures as retryable, skippable, or requiring escalation

## Architecture

### `scheduler/job.go`

Defines the `Job` model and its lifecycle states:

* `pending`
* `running`
* `retrying`
* `succeeded`
* `failed`
* `escalated`

### `scheduler/queue.go`

Implements a per-tenant priority queue using Go’s `container/heap`.

Jobs are ordered by:

1. **higher priority first**
2. **earlier submission time as tie-breaker**

### `scheduler/ratelimiter.go`

Implements per-tenant **token-bucket rate limiting** for job submissions.

Each tenant has a token bucket with:

* **rate** — how many tokens are refilled per second
* **capacity** — maximum burst size
* **tokens** — current available tokens
* **lastRefill** — timestamp of the last refill calculation

On each job submission:

1. the tenant’s bucket is refilled based on elapsed time
2. one token is consumed if available
3. if no token is available, the submission is rejected with a rate-limit error

This protects the scheduler from tenants that try to flood it with a large number of jobs in a short time.

### `scheduler/scheduler.go`

Contains the core scheduling engine.

Key responsibilities:

* `Submit(job)` enforces tenant-level token-bucket rate limiting and then adds the job to the correct tenant queue
* `nextJob()` selects the next runnable job by scanning tenant queues in round-robin order
* a worker pool executes jobs using a user-provided `JobHandler`
* failed jobs are retried up to `MaxRetries` using exponential backoff plus jitter

Backoff formula:

```go id="q7c5u8"
2^attempt * 100ms + jitter
```

If a job is still inside its backoff window, the scheduler skips it for now and checks other tenants instead of blocking on it.

### `scheduler/agent.go`

Contains the retry-decision layer.

Instead of blindly retrying every failure, the scheduler can ask a retry agent to classify the failure into one of three actions:

* **`retry`** — the failure looks transient (for example: timeout, rate limit, temporary service failure)
* **`skip`** — the failure looks permanent (for example: validation error or bad input)
* **`escalate`** — the failure is ambiguous and should be flagged for human review

If an Anthropic API key is configured, the retry agent uses the model to make this decision. If no API key is present, the scheduler safely falls back to always retrying, so the core scheduler remains functional without any external dependency.

### `api/handlers.go`

Provides a thin HTTP layer for:

* submitting a job
* fetching a job by ID
* listing all jobs for a tenant

If a tenant exceeds its configured submission rate, the API returns **HTTP 429 Too Many Requests**.

### `main.go`

Wires the scheduler to an HTTP server running on `:8080` and configures default / tenant-specific rate limits.

## Retry decision layer

To enable the LLM-based retry agent:

```bash id="6cm0n1"
export ANTHROPIC_API_KEY=your_key_here
go run .
```

If the key is not set, the scheduler falls back to the default behavior and retries failed jobs up to `MaxRetries`.

This makes the retry agent **optional** rather than a hard dependency.

## Running the project

```bash id="3utpkd"
go run .
```

## Configuring rate limits

The scheduler supports:

* a **default rate limit** applied to all tenants
* **tenant-specific overrides** for premium or high-throughput tenants

Example:

```go id="04xwov"
sched.SetDefaultRateLimit(5, 10)          // 5 jobs/sec, burst 10 for all tenants
sched.SetTenantRateLimit("premium", 20, 40) // premium tenant override
```

This means:

* a tenant can submit jobs at a sustained rate of `rate` jobs per second
* a tenant can temporarily burst up to `capacity` jobs if tokens are available

## Example usage

### Submit a job

```bash id="du3s9q"
curl -X POST localhost:8080/jobs/submit \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"acme","payload":"send-welcome-email","priority":5,"max_retries":3}'
```

If the tenant exceeds its rate limit, the request returns:

```text id="c9j6kq"
HTTP/1.1 429 Too Many Requests
```

### Get job status

```bash id="pk2d1w"
curl "localhost:8080/jobs/get?id=<job_id>"
```

### List jobs for a tenant

```bash id="o3vx7g"
curl "localhost:8080/tenants/jobs?tenant_id=acme"
```

## Running tests

```bash id="wdpl3x"
go test ./...
```

Current tests cover:

* successful job execution
* retry + backoff behavior on repeated failure
* early stop on agent-driven `skip`
* escalation on agent-driven `escalate`
* fairness across tenants under uneven queue sizes
* tenant submission rate limiting via token bucket

## Current scope and limitations

This project is intentionally a focused scheduler prototype rather than a full production job platform.

### Current scope

* in-memory job storage
* per-tenant queue isolation
* round-robin fairness across tenants
* priority scheduling within each tenant
* per-tenant token-bucket rate limiting on submission
* retry with backoff and jitter
* optional LLM-based retry classification

### Not implemented

* persistent job storage (Postgres / Redis / SQLite)
* distributed workers or leader election
* per-tenant concurrency caps
* job deduplication / idempotency guarantees
* metrics, tracing, or dashboards
* delayed queue optimization beyond simple polling

## Why this project is interesting

This project explores a specific multi-tenant systems problem: **how to keep background work fair and controlled across tenants in a shared scheduler**.

It is not trying to be a full workflow engine or production-grade distributed queue. Instead, it focuses on a few core concerns that show up often in multi-tenant backend systems:

* isolation of work by tenant
* fairness under uneven load
* protection from noisy tenants flooding the system
* retry behavior under failure
* policy-driven handling of ambiguous failures

## Possible extensions

* Add per-tenant concurrency caps so one tenant cannot occupy all workers
* Persist jobs to Postgres or Redis
* Replace the polling worker loop with a channel-based dispatcher
* Add metrics for queue depth, retries, rate-limit rejections, and per-tenant throughput
* Expose job status updates over streaming APIs instead of polling
