# Testing Dispatcher

This covers three levels: unit tests (pure logic, no infra), local
integration testing (docker-compose + manual GraphQL calls), and — the
most useful part for a portfolio project — **deliberately breaking things**
to watch the resilience mechanisms actually engage.

## 1. Unit tests

No Docker required. These cover the logic that has to be correct in
isolation: the state machine, backoff/jitter math, and HMAC signing.

```bash
go mod tidy   # first time only; needs network to resolve dependencies
make test
# or directly:
go test ./... -v -race -cover
```

What's covered and why it's the right thing to unit test:
- `internal/delivery/state_machine_test.go` — pins down every legal
  transition and, just as importantly, asserts specific illegal ones are
  rejected (you cannot move a `SUCCEEDED` job back to `DELIVERING`).
- `internal/delivery/backoff_test.go` — asserts backoff grows, is capped,
  and is jittered (not identical across repeated calls at the same
  attempt number) — the property that actually prevents a thundering herd.
- `internal/delivery/signer_test.go` — a valid signature verifies, a wrong
  secret fails, a tampered payload fails.

Anything that talks to Postgres or Redis is deliberately *not* unit
tested with mocks — mocking `pgx` tends to produce tests that pass
against the mock and fail against real Postgres (e.g. `FOR UPDATE SKIP
LOCKED` semantics are meaningless against a mock). Those are covered by
integration testing below instead.

## 2. Local integration setup

```bash
cp .env.example .env
go mod tidy
make up          # postgres + redis + webhook-sink, migrations applied
make api         # terminal 1
make worker      # terminal 2
```

Seed a tenant (no mutation for this yet — intentionally, since tenant
creation/auth is a documented next step, see CURSOR_CONTEXT.md):

```bash
docker compose exec -T postgres psql -U dispatcher -d dispatcher -c \
  "INSERT INTO tenants (id, name, api_key) VALUES ('00000000-0000-0000-0000-000000000001', 'Acme Inc', 'test_key') ON CONFLICT DO NOTHING;"
```

Open `http://localhost:8080/graphql` (GraphiQL) and run:

```graphql
mutation CreateEndpoint {
  createEndpoint(
    tenantId: "00000000-0000-0000-0000-000000000001"
    url: "http://webhook-sink:9000/"
    eventTypes: ["order.created"]
  ) { id url circuitState }
}
```

```graphql
mutation Publish {
  publishEvent(
    tenantId: "00000000-0000-0000-0000-000000000001"
    eventType: "order.created"
    payload: "{\"orderId\": \"abc123\"}"
    idempotencyKey: "order-abc123-created"
  ) { id status }
}
```

Watch it arrive: `docker compose logs -f webhook-sink` — you should see a
POST with an `X-Dispatcher-Signature` header within a second or two.

Check delivery status:

```graphql
query {
  deliveryStatus(eventId: "<event id from publish response>") {
    endpointId status attemptCount lastError
  }
}
```

## 3. Proving idempotency

Run the exact same `publishEvent` mutation twice with the same
`idempotencyKey`. Expected: both calls return the **same** event `id`,
and `deliveryStatus` shows exactly one delivery attempt sequence per
endpoint, not two. Check directly:

```sql
SELECT id, idempotency_key, created_at FROM events WHERE idempotency_key = 'order-abc123-created';
-- exactly one row, regardless of how many times you published
```

## 4. Proving retries + backoff

Stop the webhook sink so deliveries fail:

```bash
docker compose stop webhook-sink
```

Publish an event. Watch the worker logs — you'll see `delivery attempt
failed`, then increasing gaps between retries as backoff grows
(check `next_attempt_at` in `delivery_jobs` between attempts):

```sql
SELECT attempt_count, status, next_attempt_at, last_error FROM delivery_jobs ORDER BY updated_at DESC LIMIT 5;
```

Bring the sink back up (`docker compose start webhook-sink`) before
`max_attempts` (default 8) is reached, and the next retry should succeed.
If you let it exhaust all 8 attempts, confirm it lands in
`DEAD_LETTERED`:

```sql
SELECT status, attempt_count FROM delivery_jobs WHERE status = 'DEAD_LETTERED';
```

## 5. Proving the circuit breaker

With the sink stopped, publish 5+ events to the same endpoint in a short
window. After 5 consecutive failures the endpoint should trip to `OPEN`:

```sql
SELECT id, circuit_state, circuit_failure_count, circuit_opened_at FROM endpoints;
```

While `OPEN`, new delivery attempts for that endpoint should stop showing
up in worker logs entirely (they're skipped, not attempted) until the 60s
cooldown elapses, at which point you'll see exactly one probe attempt
(`HALF_OPEN`) before it either closes (sink back up) or reopens (still
down).

## 6. Proving crash recovery (stale job reclaim)

1. Publish an event so a `delivery_jobs` row is claimed (`DELIVERING`).
2. Kill the worker process mid-flight (`Ctrl+C` the `make worker`
   terminal right after publishing, or `kill -9` if you want to simulate
   a hard crash with no graceful shutdown).
3. Manually confirm the job is stuck: `SELECT status, locked_by, locked_at
   FROM delivery_jobs;` — it'll show `DELIVERING` with no worker actually
   working on it.
4. Restart the worker. Within the 30s reaper interval
   (`internal/queue/queue.go`'s `ReleaseStale`, wired up in
   `cmd/worker/main.go`), the job should flip back to `PENDING` and get
   reclaimed — confirm delivery still eventually succeeds.

## 7. Proving reconciliation catches drift

Reconciliation compares `delivery_attempt_logs` against `usage_ledger`.
To manually create drift for testing purposes:

```sql
DELETE FROM usage_ledger WHERE id = (SELECT id FROM usage_ledger LIMIT 1);
```

Run the reconciler:

```bash
make reconciler
```

Check the report:

```sql
SELECT * FROM reconciliation_reports ORDER BY run_at DESC LIMIT 1;
```

`missing_from_ledger` should be `1` (or more), and the
`dispatcher_reconciliation_mismatches` Prometheus gauge (visible at
`http://localhost:8080/metrics` after the next API-side scrape, or check
the reconciler's own process metrics if you wire that up) should reflect
it.

## 8. Load-testing the concurrency guarantees (optional, more advanced)

To actually observe `FOR UPDATE SKIP LOCKED` doing its job: start 3+
worker processes (`make worker` in 3 terminals, or scale via
`docker compose` if you containerize `cmd/worker`), publish a batch of 50+
events, and confirm via logs / `locked_by` values that jobs are spread
across worker IDs with no job ever claimed by two workers at once (you'd
see this as a duplicate `delivery_attempt_logs` entry with the same
`attempt_number` for the same job, which should never happen — assert it
with `SELECT delivery_job_id, attempt_number, COUNT(*) FROM
delivery_attempt_logs GROUP BY 1,2 HAVING COUNT(*) > 1;` returning zero
rows).

## Known gaps in test coverage (see CURSOR_CONTEXT.md)

- No automated integration test suite yet (the above is manual/scripted
  by hand) — a natural next step is a `test/integration` package using
  `testcontainers-go` against real Postgres + Redis, run in CI.
- GraphQL resolver tests exist implicitly via the manual flows above but
  have no automated coverage yet.
