# Architecture: principles → code

This document exists because the code should never be more authoritative
than the reasoning behind it. Each section below is one of the design
principles this project set out to demonstrate, what it means concretely
in a webhook-delivery system, and exactly where in the codebase it's
implemented.

## System shape

```
                    ┌──────────────┐
   producers  ───▶  │  cmd/api      │  GraphQL, stateless, N replicas
  (internal         │  (publishEvent)│
   services)        └──────┬───────┘
                            │ 1 transaction: write event + fan-out jobs
                            ▼
                    ┌──────────────┐
                    │  Postgres     │  events, delivery_jobs,
                    │  (source of   │  delivery_attempt_logs, endpoints
                    │   truth)      │
                    └──────┬───────┘
                            │ SELECT ... FOR UPDATE SKIP LOCKED
                            ▼
                    ┌──────────────┐
                    │  cmd/worker   │  stateless, N replicas, N goroutines each
                    │  (delivery)   │──▶ customer endpoints (HTTP, signed, timeout)
                    └──────┬───────┘
                            │ publishes domain events
                            ▼
                    ┌──────────────┐
                    │ usage_ledger  │  separate bounded context (billing),
                    │ (async,       │  eventually consistent with delivery
                    │  idempotent)  │
                    └──────┬───────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ cmd/reconciler│  scheduled: reconcile + archive
                    └──────────────┘
```

---

## 1. Transactions wrap multi-write operations

**Principle:** if an operation spans multiple writes, wrap it in a
transaction — atomicity is a requirement, not an optimization.

**Where:** `internal/outbox/outbox.go`, `PublishEvent`. Writing the
`events` row and creating one `delivery_jobs` row per subscribed endpoint
happens inside one `pgx.Tx`. If fan-out fails partway (say, endpoint 3 of
5 fails to insert), the whole transaction rolls back — you never end up
with an event that's "half fanned out."

Same pattern in `internal/queue/queue.go`: `MarkSucceeded` and
`MarkFailed` each update `delivery_jobs.status` *and* insert into
`delivery_attempt_logs` in one transaction, so the current state and the
audit trail can never drift apart.

## 2. External-service work goes through a queue

**Principle:** if a task depends on an external service, move it to a
queue — network I/O is unpredictable, your request lifecycle shouldn't be.

**Where:** `publishEvent` (the request a producer makes) never calls a
customer's HTTP endpoint. It commits a transaction and returns.
`internal/queue/queue.go` is the queue: `delivery_jobs` rows, claimed by
`Claim()` using `SELECT ... FOR UPDATE SKIP LOCKED`. The actual HTTP call
happens later, in `internal/delivery/worker.go`, on a completely separate
process (`cmd/worker`) with its own timeout and retry budget. A producer
publishing 500 events to a customer whose server is down sees zero
latency impact.

*Why Postgres instead of Kafka/SQS/NATS:* the queue's state (`PENDING` /
`DELIVERING` / etc.) has to be transactionally consistent with the audit
log and the circuit breaker state, all of which also live in Postgres.
Keeping them in one system avoids a distributed-consistency problem
between the queue and the database. The tradeoff is throughput ceiling —
documented as a deliberate v1 choice in `docs/PRD.md` §7, not an
oversight. Swapping the `Queue` interface for a Kafka-backed
implementation later is possible without touching the state machine or
worker logic.

## 3. Idempotency

**Principle:** if an operation may be executed more than once, design it
to be idempotent — retries should preserve correctness, not create
duplicate side effects.

This system has idempotency at **three separate layers**, because
duplication can be introduced at three separate points:

- **Producer → API:** `events.idempotency_key` has a real Postgres
  `UNIQUE (tenant_id, idempotency_key)` constraint (migration
  `0001_init.sql`). `outbox.PublishEvent` catches the resulting `23505`
  error (`internal/idempotency/idempotency.go`) and returns the
  *original* event instead of erroring — a producer that retries a
  timed-out publish gets the same result both times, with zero duplicate
  fan-out. This is a database constraint, not an app-level
  check-then-insert, because check-then-insert has a race window.
- **Worker → downstream billing consumer:** `usage_ledger` has
  `UNIQUE (event_id, endpoint_id)` and the subscriber in `cmd/worker/main.go`
  uses `ON CONFLICT DO NOTHING`. Domain events on the internal bus
  (`internal/events`) can be redelivered or double-published; the
  consumer is written assuming that.
- **Dispatcher → receiver (documented contract, not enforced by us):**
  every delivered payload includes the event `id`. Because delivery
  semantics are at-least-once (a receiver can 200 an outbound HTTP call
  that then times out on our end before we see the response — see
  worker.go's `deliver`), receivers are told in this doc to dedupe on
  event ID. We can't force that server-side; documenting the contract
  clearly is the correct scope for this system.

## 4. Concurrency control

**Principle:** if multiple requests can mutate the same resource, design
for concurrency — optimistic locking, pessimistic locking, or atomic
updates where appropriate.

- **Pessimistic locking:** `Queue.Claim` (`internal/queue/queue.go`) uses
  `FOR UPDATE SKIP LOCKED` so multiple worker processes polling the same
  table never claim the same job twice, and never block waiting on a row
  another worker already has.
- **Optimistic locking:** `endpoints.version` (migration) plus
  `CircuitBreaker.RecordResult` (`internal/delivery/circuitbreaker.go`).
  Two workers can finish delivery attempts for the same endpoint at
  nearly the same moment; each read-modify-write of the circuit breaker
  state includes `WHERE version = $n` and retries (bounded, 3 attempts)
  on a version mismatch rather than silently overwriting a concurrent
  update.
- **Atomic updates:** `ON CONFLICT ... DO NOTHING` for fan-out job
  creation and ledger writes avoids read-then-write races entirely where
  a single statement can express the intent.

## 5. Server-side validation

**Principle:** if the client provides business-critical data, validate it
on the server — clients are untrusted execution environments.

**Where:** every GraphQL mutation resolver in
`internal/graphql/resolvers.go` revalidates its inputs even though a real
frontend would also validate them client-side: `createEndpoint` parses
and checks the URL scheme and requires at least one event type;
`publishEvent` requires a non-empty `eventType`, a non-empty
`idempotencyKey`, and checks `json.Valid` on the payload before it ever
touches the database. None of this trusts that the caller (which could be
a script, a misconfigured internal service, or a malicious actor with a
tenant's API key) sent well-formed data.

## 6. Explicit state machines

**Principle:** if a workflow modifies state, model it as explicit state
transitions — well-defined state machines eliminate invalid transitions.

**Where:** `internal/delivery/state_machine.go` defines the full legal
transition table for `models.JobStatus`
(`PENDING → DELIVERING → SUCCEEDED`, `... → FAILED → PENDING | DEAD_LETTERED`,
etc.) and `CanTransition`/`ValidateTransition` are the only sanctioned way
to reason about whether a move is legal. Every place that changes
`delivery_jobs.status` (queue.go's `Claim`, `MarkSucceeded`, `MarkFailed`,
and the stale-job reaper) does so along one of these edges. Tests in
`internal/delivery/state_machine_test.go` pin the table down, including
the illegal moves (you cannot resurrect a `SUCCEEDED` or
`DEAD_LETTERED` job).

## 7. Caching + invalidation

**Principle:** if the same computation is performed repeatedly, introduce
caching; if cached data becomes stale, define an invalidation strategy as
part of system design.

**Where:** `internal/cache/cache.go` implements cache-aside reads of
endpoint config for the GraphQL read path (`ResolveEndpoint`). The
invalidation strategy is explicit and synchronous: every mutation that
changes an endpoint (`ResolveUpdateEndpoint`) calls `InvalidateEndpoint`
in the same request, before returning success — not "eventually," not
relying on the 5-minute TTL alone to paper over staleness. The TTL exists
only as a safety net for bugs or out-of-band writes, not as the primary
consistency mechanism.

Deliberately **not** cached: the worker's read of endpoint config
(`worker.go`'s `loadContext`) always goes straight to Postgres. Circuit
breaker state changes on every attempt and must never be read stale —
caching it would mean a worker could keep hammering an endpoint whose
breaker just tripped in another goroutine.

## 8. Resilience against a degraded downstream

**Principle:** if a downstream dependency is degraded, apply timeouts,
retries with exponential backoff, and circuit breakers — prevent
cascading failures.

**Where, together, in `internal/delivery/worker.go` and
`circuitbreaker.go`:**
- **Timeout:** each worker's `http.Client` has an explicit `Timeout`
  (`cfg.HTTPClientTimeout`, default 5s) — a hung customer endpoint can
  only ever cost a worker 5 seconds, never a goroutine forever.
- **Exponential backoff with full jitter:** `backoffWithJitter` in
  worker.go grows `2s * 2^(attempt-1)`, capped at 10 minutes, with
  randomized jitter so many jobs failing simultaneously (one endpoint
  going down) don't all retry in lockstep the moment their window opens.
  `internal/delivery/backoff_test.go` asserts both properties.
- **Circuit breaker:** `CircuitBreaker.Allow`/`RecordResult`
  (`circuitbreaker.go`) trips an endpoint to `OPEN` after 5 consecutive
  failures, refuses further attempts for a 60s cooldown, then allows one
  `HALF_OPEN` probe. State lives on the `endpoints` row, not in worker
  memory, so it's correct across every worker replica.

## 9. Asynchronous execution for non-urgent work

**Principle:** if work doesn't require an immediate response, execute it
asynchronously — keep request-response paths short and predictable.

**Where:** `publishEvent`'s entire cost to the caller is one DB
transaction (milliseconds). Delivery — which can take up to 5 seconds per
attempt, times up to 8 attempts, times however many endpoints are
subscribed — happens entirely out of band in `cmd/worker`. See system
diagram above.

## 10. Domain events across bounded contexts

**Principle:** if an event affects multiple bounded contexts, publish a
domain event — avoid tight coupling between services.

**Where:** `internal/events/publisher.go` is an internal pub/sub bus. The
delivery worker (`worker.go`) doesn't know or care that a billing system
exists; it just publishes `delivery.succeeded` / `delivery.dead_lettered`.
The usage-metering subscriber, wired up in `cmd/worker/main.go`, is the
only thing that knows those events matter for billing, and it writes to
its *own* table (`usage_ledger`) rather than the delivery worker writing
to it directly. This is what makes it correct to describe delivery and
billing as two separate bounded contexts instead of one system that does
two things.

## 11. Eventual consistency, by design

**Principle:** if consistency across services isn't immediate, design for
eventual consistency — distributed systems trade immediacy for
availability.

**Where:** the gap between `delivery_attempt_logs` (updated synchronously
by the worker that made the attempt) and `usage_ledger` (updated
asynchronously by a domain-event subscriber) is not a bug — it's the
explicit tradeoff described in principle #10. `internal/reconcile` exists
*because* this gap is real and deliberate, not despite it.

## 12. Persist intent before an irreversible side effect

**Principle:** if the operation produces an irreversible side effect,
persist intent before execution — recovery starts with durable state.

**Where:** the irreversible side effect here is an outbound HTTP POST to
a customer's server (you can't un-send it). `outbox.PublishEvent` durably
persists the `delivery_jobs` row — the *intent* to deliver — before any
HTTP call is ever attempted. If every worker process crashes the instant
after `PublishEvent` commits, the intent survives in Postgres and gets
picked up by the next worker that starts. Nothing about "have I sent
this yet" is held in memory anywhere.

## 13. Reconciliation as a core workflow

**Principle:** if money is involved, treat reconciliation as a core
workflow — every financial system should be able to answer "what
actually happened?"

**Where:** `internal/reconcile/reconcile.go`. Compares
`delivery_attempt_logs` (ground truth: what was actually delivered) against
`usage_ledger` (what billing thinks was delivered) for a time window,
producing `MissingFromLedger` / `OrphanedInLedger` lists, persisting a
`reconciliation_reports` row, and exposing a
`dispatcher_reconciliation_mismatches` gauge. This is the literal answer
to "what actually happened" — it doesn't trust either side blindly.

## 14. Idempotent, duplicate/out-of-order-tolerant consumers

**Principle:** if processing depends on event delivery, assume
duplicates, delays and out-of-order messages — consumers should be
idempotent.

**Where:** the `usage_ledger` subscriber (`cmd/worker/main.go`) uses
`ON CONFLICT (event_id, endpoint_id) DO NOTHING` — processing the same
`delivery.succeeded` event twice is a no-op, not a double-charge. The
`Queue.Claim` mechanism itself is what prevents *delivery* jobs from being
processed twice concurrently (`FOR UPDATE SKIP LOCKED`), and
`ReleaseStale` handles the "worker died mid-processing" case by returning
orphaned `DELIVERING` jobs to `PENDING` rather than leaving them stuck.

## 15. Statelessness for horizontal scale

**Principle:** if the system must scale, eliminate shared bottlenecks —
horizontal scalability begins with stateless services.

**Where:** `cmd/api` and `cmd/worker` hold zero in-process state that
matters across requests/jobs — every piece of coordination-relevant state
(circuit breaker, queue position, cache) lives in Postgres or Redis.
`config.Load()` reads everything from environment variables
(`internal/config/config.go`), so the exact same binary can be started N
times with no per-instance configuration. Run 1 API replica or 20; run 1
worker or 50 — the correctness guarantees (from `FOR UPDATE SKIP LOCKED`
and the state machine) hold regardless.

## 16. Independent read/write path optimization

**Principle:** if data access becomes a bottleneck, optimize read and
write paths independently — not every workload deserves the same
architecture.

**Where:** the write path (`outbox.PublishEvent`) is optimized for
correctness and low latency on a small number of rows per call. The read
path (`ResolveEndpoint`) is optimized for repeat-read speed via the Redis
cache-aside layer, completely independent of how writes work. They don't
share a code path or a consistency mechanism beyond the explicit
invalidation call — which is exactly what "not every workload deserves
the same architecture" means in practice.

## 17. Observability

**Principle:** if failures aren't observable, you don't have a reliable
system — invest in structured logging, metrics, tracing and alerting.

**Where:** `internal/observability/observability.go` — Prometheus
counters/histograms/gauges for delivery outcomes, latency, open circuit
breakers, queue depth, and reconciliation mismatches; `zap` structured
logging throughout every package (never `fmt.Println`); `/healthz`
(process is up) vs. `/readyz` (dependencies are reachable) exposed
separately in `cmd/api/main.go`, which matters for how an orchestrator
should treat a starting-but-not-ready pod versus a genuinely dead one.

## 18. Explicit failure paths between services

**Principle:** if a service communicates with another service, design
explicit failure paths — every dependency has a failure mode.

**Where:** the worker → customer-endpoint dependency has three explicit
failure paths, not one generic "catch and log": timeout (network hang),
non-2xx response (endpoint is up but rejecting), and connection refused
/DNS failure (endpoint is fully down) — all three flow into the same
retry/backoff/circuit-breaker logic in `worker.go`, but are logged with
distinct error messages so an operator can tell which one is happening.
The worker → Postgres dependency's failure path is the stale-job reaper
(`ReleaseStale`) — a worker crashing mid-delivery has an explicit recovery
path, not an assumption that workers never crash.

## 19. Archival

**Principle:** if data is no longer needed operationally, archive it —
operational databases shouldn't become long-term storage.

**Where:** `internal/archive/archive.go`, run on a schedule by
`cmd/reconciler`. Moves `delivery_attempt_logs` rows older than
`ARCHIVE_AFTER` (default 90 days) into `archived_delivery_attempt_logs` in
batches of 5,000 inside short transactions with `FOR UPDATE SKIP LOCKED`,
so a multi-year backlog never holds one long transaction or blocks the
hot table's writers.

## 20. Resilience as a design property

**Principle:** if a system cannot recover gracefully from failure, it
isn't production-ready — resilience is a design property, not a
deployment milestone.

**Where:** this is the sum of everything above, plus graceful shutdown in
both `cmd/api/main.go` (drains in-flight HTTP requests before exiting) and
`cmd/worker/main.go` (cancels the worker context, waits for in-flight
deliveries to finish via `sync.WaitGroup`, before exiting) — a `SIGTERM`
during a rolling deploy behaves the same as any other planned event, not
a special case.

---

## What's deliberately simplified for a v1 reference implementation

Being direct about this matters more than pretending otherwise:

- **Auth** is a bare tenant API key concept sketched in the schema
  (`tenants.api_key`), not wired into GraphQL middleware yet. See
  `docs/CURSOR_CONTEXT.md` for the next-step plan.
- **Tracing** (OpenTelemetry spans across the API → queue → worker →
  billing hop) is not implemented — metrics and logs are, tracing is
  listed as a next step.
- **Rate limiting per tenant** is not implemented — a single tenant
  publishing at very high volume could currently starve queue capacity
  for others. Noted as a known gap, not silently ignored.
- **The Postgres-native queue** trades ingestion throughput ceiling for
  transactional consistency with the rest of the system state — the right
  choice for this project's goals, but worth naming explicitly rather
  than presenting it as the only correct design.
