# Agent context: resuming work on Dispatcher

Purpose of this file: if you (a human, or an AI coding agent like Cursor,
Claude Code, etc.) are picking this project up in a fresh session with no
memory of how it was built, this is everything you need to keep going
without re-deriving decisions that were already made deliberately.

Read this file first, then `docs/ARCHITECTURE.md` for the reasoning
behind the existing code, before changing anything.

## Project identity

- **Name:** Dispatcher — a reliable webhook delivery platform.
- **Repo:** https://github.com/Zubimendi/Dispatcher
- **Module:** `github.com/Zubimendi/Dispatcher`
- **Stack:** Go 1.22, GraphQL (`graphql-go/graphql`, hand-assembled
  schema, no codegen), PostgreSQL (source of truth + job queue via
  `FOR UPDATE SKIP LOCKED`), Redis (cache-aside for endpoint reads only).
- **Purpose:** portfolio / reference implementation of production backend
  design principles (transactions, idempotency, concurrency control,
  circuit breakers, state machines, caching+invalidation, async
  processing, domain events, eventual consistency, reconciliation,
  archival, observability). See `docs/PRD.md` for the full brief and
  `docs/ARCHITECTURE.md` for the principle→code mapping. **Do not remove
  or simplify away the patterns documented there without updating that
  doc to match** — the documentation and the code are meant to stay in
  lockstep; that's the entire value of this project as a portfolio piece.

## Current state (as of this handoff)

### Done
- Full DB schema (`internal/db/migrations/0001_init.sql`): tenants,
  endpoints (with circuit breaker + optimistic-lock version columns),
  events (outbox, with idempotency unique constraint), delivery_jobs (the
  queue), delivery_attempt_logs (audit trail), archived_delivery_attempt_logs,
  usage_ledger (separate bounded context), reconciliation_reports.
- `internal/outbox` — transactional publish + idempotent-duplicate
  handling.
- `internal/queue` — Claim/MarkSucceeded/MarkFailed/ReleaseStale, all
  using SKIP LOCKED and transactions.
- `internal/delivery` — state machine (`state_machine.go`), circuit
  breaker with optimistic locking (`circuitbreaker.go`), HMAC signer
  (`signer.go`), and the worker itself (`worker.go`) with timeouts,
  backoff+jitter, and dead-lettering.
- `internal/cache` — cache-aside endpoint reads with invalidate-on-write.
- `internal/events` — in-process domain event bus.
- `internal/reconcile` — reconciliation job comparing delivery logs to
  usage ledger, persists a report row.
- `internal/archive` — batched archival of old attempt logs.
- `internal/observability` — Prometheus metrics + zap logging + health
  endpoints.
- `internal/graphql` — schema + resolvers for `createEndpoint`,
  `updateEndpoint`, `publishEvent`, `endpoint` query, `deliveryStatus`
  query, with server-side validation on every mutation.
- `cmd/api`, `cmd/worker`, `cmd/reconciler` — three entrypoints,
  stateless/config-via-env. API and worker shut down gracefully on
  SIGINT/SIGTERM; reconciler is a one-shot (cron-friendly) that runs
  reconcile + archive then exits. Worker wires the `usage_ledger`
  subscriber and a `ReleaseStale` reaper.
- Module path: `github.com/Zubimendi/Dispatcher` (repo:
  https://github.com/Zubimendi/Dispatcher). `go.sum` present; `go build
  ./...`, `go vet ./...`, and `go test ./... -race` pass.
- Unit tests for state machine, backoff jitter, HMAC signing
  (`internal/delivery/*_test.go`).
- `docker-compose.yml` (Postgres, Redis, a webhook-sink for manual
  testing), `Makefile`, `.env.example`.
- Docs: `README.md`, `docs/PRD.md`, `docs/ARCHITECTURE.md`,
  `docs/TESTING.md`, `docs/STORY.md`, this file.

### NOT done — verify before assuming it works

- **Tenant creation/auth has no mutation or middleware yet.** There's a
  `tenants` table with an `api_key` column but nothing enforces it. Next
  step: add a `createTenant` mutation (or seed via migration for local
  dev) and an HTTP middleware in `cmd/api/main.go` that resolves an
  `Authorization` header to a tenant ID and injects it into
  `graphql.Params.Context`, then have resolvers read the tenant ID from
  context instead of trusting a `tenantId` argument from the client (the
  current `tenantId` argument on every mutation is a placeholder for this
  — it should be removed once real auth exists, since accepting tenant
  ID as a client-supplied argument defeats the point of multi-tenancy).
- **No automated integration test suite.** `docs/TESTING.md` documents a
  manual/scripted procedure. Next step: `test/integration` using
  `testcontainers-go` (spins up real Postgres + Redis in Docker for the
  test run) covering: idempotent publish, retry+backoff reaching a
  successful delivery, circuit breaker tripping and recovering, stale-job
  reclaim, and a reconciliation run that correctly flags injected drift.
- **No rate limiting per tenant.** A noisy tenant can currently publish
  enough events to dominate queue capacity for everyone. Natural next
  step: a per-tenant token bucket, checked in `ResolvePublishEvent`
  before the outbox write, likely backed by Redis (`INCR` + `EXPIRE`).
- **No tracing.** Metrics and logs exist; OpenTelemetry spans across
  API → queue claim → HTTP delivery → domain event → ledger write do not.
  Would meaningfully help debug "why did this specific event take 40
  seconds to deliver" in a way logs alone don't.
- **`internal/graphql/util.go`'s `randomHex`** uses `crypto/rand` but
  doesn't check the error from `rand.Read` — acceptable for a webhook
  secret (failure here just means a less-random secret, not a security
  bypass, and `crypto/rand.Read` essentially never fails on Linux) but
  flag it if a linter complains; not worth over-engineering.
- **No Dockerfiles for `cmd/api` / `cmd/worker` / `cmd/reconciler`
  themselves** — `docker-compose.yml` only runs their dependencies
  (Postgres, Redis, webhook-sink); the Go binaries run via `go run`
  locally. Next step if deploying anywhere: multi-stage Dockerfiles per
  binary, added to docker-compose.yml as additional services.

## Design decisions already made — don't relitigate these without reason

These were deliberate, documented in `docs/ARCHITECTURE.md`, and
changing them should come with an update to that doc, not just a diff:

1. **Postgres-native queue, not Kafka/NATS/SQS.** Chosen for
   transactional consistency between queue state, circuit breaker state,
   and audit log, at the cost of throughput ceiling. If a future
   requirement genuinely needs 10k+ events/sec ingestion, the right move
   is probably: keep Postgres as the durable outbox, add a Kafka consumer
   that reads from a logical replication slot or a CDC tool (Debezium) to
   fan out at higher throughput — not to rip out the transactional
   guarantees.
2. **Circuit breaker state lives on the `endpoints` row, not in worker
   memory or Redis.** Needs to be correct across worker replicas and
   survive restarts; Postgres with optimistic locking (`version` column)
   was chosen over Redis specifically so it's covered by the same
   transactional guarantees as everything else it interacts with
   (delivery_jobs updates happen in the same logical flow).
3. **Three separate idempotency mechanisms** (producer/DB constraint,
   consumer/ON CONFLICT, receiver/documented contract) are intentional,
   not redundant — each closes a different duplication window. See
   ARCHITECTURE.md §3.
4. **`graphql-go/graphql` instead of `gqlgen`.** Chosen so the schema is
   hand-assembled Go code (readable top-to-bottom in `schema.go`) rather
   than generated from an SDL file requiring a codegen step — trades some
   type safety (gqlgen generates typed resolvers) for zero build-step
   friction and easier reading for a portfolio audience. If this project
   grows a large schema, revisit — gqlgen scales better past a handful of
   types.

## Suggested next-session priorities, in order

1. Stand up `docker compose up` and walk through `docs/TESTING.md`
   end-to-end by hand once, to confirm the documented behavior actually
   matches reality — fix either the code or the docs, whichever is wrong.
2. Add tenant auth (item above) — this is the biggest correctness gap for
   anyone actually trying to run this multi-tenant.
3. Add the `test/integration` suite with testcontainers-go.
4. Then, in rough priority order: rate limiting, tracing, Dockerfiles for
   deployment, a `createTenant` mutation.

## How to give a fresh agent session everything it needs

Point it at, in this order: this file → `docs/ARCHITECTURE.md` →
`docs/PRD.md`. Tell it explicitly: "don't remove a resilience pattern to
make a build error go away without checking ARCHITECTURE.md for why it's
there."
