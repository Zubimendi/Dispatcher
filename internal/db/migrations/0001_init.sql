-- Dispatcher schema
-- Design notes are in docs/ARCHITECTURE.md. Key ideas encoded here:
--   * events table = the transactional outbox ("persist intent before execution")
--   * idempotency enforced via a real unique constraint, not app-level checks
--   * delivery_jobs is a Postgres-native queue (SELECT ... FOR UPDATE SKIP LOCKED)
--   * every state field has an explicit, bounded set of values (state machine)

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    api_key     TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url                  TEXT NOT NULL,
    secret               TEXT NOT NULL,
    event_types          TEXT[] NOT NULL,
    is_active            BOOLEAN NOT NULL DEFAULT true,
    circuit_state        TEXT NOT NULL DEFAULT 'CLOSED'
                          CHECK (circuit_state IN ('CLOSED','OPEN','HALF_OPEN')),
    circuit_failure_count INT NOT NULL DEFAULT 0,
    circuit_opened_at    TIMESTAMPTZ,
    version               INT NOT NULL DEFAULT 1,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_endpoints_tenant ON endpoints(tenant_id);

CREATE TABLE events (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL,
    payload          JSONB NOT NULL,
    idempotency_key  TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING','DISPATCHED','COMPLETED','FAILED')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX idx_events_tenant_status ON events(tenant_id, status);

CREATE TABLE delivery_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id      UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status           TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING','DELIVERING','SUCCEEDED','FAILED','DEAD_LETTERED')),
    attempt_count    INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 8,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by        TEXT,
    locked_at        TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);

CREATE INDEX idx_delivery_jobs_claimable
    ON delivery_jobs (next_attempt_at)
    WHERE status = 'PENDING';

CREATE TABLE delivery_attempt_logs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_job_id  UUID NOT NULL REFERENCES delivery_jobs(id) ON DELETE CASCADE,
    attempt_number   INT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('SUCCEEDED','FAILED')),
    http_status      INT,
    error            TEXT,
    duration_ms      INT,
    attempted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_attempt_logs_job ON delivery_attempt_logs(delivery_job_id);
CREATE INDEX idx_attempt_logs_attempted_at ON delivery_attempt_logs(attempted_at);

CREATE TABLE archived_delivery_attempt_logs (LIKE delivery_attempt_logs INCLUDING ALL);

CREATE TABLE usage_ledger (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_id      UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id   UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    delivered_at  TIMESTAMPTZ NOT NULL,
    billed        BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (event_id, endpoint_id)
);

CREATE TABLE reconciliation_reports (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    period_start   TIMESTAMPTZ NOT NULL,
    period_end     TIMESTAMPTZ NOT NULL,
    delivered_count      INT NOT NULL,
    ledger_count         INT NOT NULL,
    missing_from_ledger  INT NOT NULL,
    orphaned_in_ledger   INT NOT NULL,
    details        JSONB NOT NULL DEFAULT '{}'::jsonb
);
