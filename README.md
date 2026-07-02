# Multi-Tenant Job Scheduler

A background job scheduler in Go that isolates work per tenant, guarantees
fairness across tenants via round-robin dispatch, and retries failed jobs
with exponential backoff + jitter.

## Why this exists

Most simple job queues process everything in one FIFO/priority queue, which
means one noisy tenant with thousands of jobs can starve every other tenant.
This scheduler keeps a separate priority queue per tenant and dispatches
work round-robin across tenants, so no tenant can monopolize the workers.

## Architecture

- **`scheduler/job.go`** — the `Job` type and its lifecycle states
  (pending → running → succeeded/retrying → failed).
- **`scheduler/queue.go`** — a per-tenant priority queue (Go's `container/heap`),
  ordered by priority then submission time.
- **`scheduler/scheduler.go`** — the core engine:
  - `Submit` enqueues a job into its tenant's isolated queue.
  - `nextJob` round-robins across tenants to pick the next runnable job,
    skipping jobs still inside their backoff window.
  - A worker pool (goroutines) pulls jobs and executes them via a
    user-supplied `JobHandler`.
  - Failed jobs are retried up to `MaxRetries` times with exponential
    backoff (`2^attempt * 100ms`) plus jitter, to avoid retry storms.
- **`scheduler/agent.go`** — an agentic retry-decision layer. Instead of
  blindly retrying every failure the same way, it calls the Anthropic API
  to classify the failure and decide: `retry` (transient — timeouts, rate
  limits), `skip` (permanent — validation errors, will never succeed no
  matter how many times you retry), or `escalate` (ambiguous — flag for a
  human instead of guessing). This is genuinely agentic: the model's
  output changes what the system does next, not just what it says.
  If no API key is set, it safely falls back to always retrying (the
  original behavior), so the scheduler never depends on an external API
  to function.
- **`api/handlers.go`** — a thin HTTP layer: submit a job, fetch a job's
  status, list a tenant's jobs.
- **`main.go`** — wires the scheduler to an HTTP server on `:8080`.

## Enabling the agentic retry layer

```bash
export ANTHROPIC_API_KEY=your_key_here
go run .
```

Without the key set, jobs retry on every failure up to `MaxRetries` (the
original behavior) — the agent layer is additive, not required.

## Running it

```bash
go run .
```

## Example usage

Submit a job:

```bash
curl -X POST localhost:8080/jobs/submit \
  -d '{"tenant_id":"acme","payload":"send-welcome-email","priority":5,"max_retries":3}'
```

Check its status:

```bash
curl "localhost:8080/jobs/get?id=<job_id_from_above>"
```

List all jobs for a tenant:

```bash
curl "localhost:8080/tenants/jobs?tenant_id=acme"
```

## Running tests

```bash
go test ./...
```

Tests cover:
- successful job execution
- retry + backoff behavior on repeated failure (agent disabled / no API key)
- agentic `skip` decision stopping retries early on a permanent error
- agentic `escalate` decision on an ambiguous error
- tenant fairness (a tenant with 1 job isn't starved behind another
  tenant's backlog of 5 jobs)

## Possible extensions

- Persist jobs to a real store (Postgres/SQLite) instead of in-memory
- Replace the polling worker loop with a channel-based dispatch for lower
  latency
- Add per-tenant rate limits, not just fairness
- Expose job status changes over gRPC streaming instead of polling
