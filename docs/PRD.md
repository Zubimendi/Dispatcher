# Dispatcher — Product Requirements Document

## 1. Problem

Any product that lets other systems react to its events — a payment
succeeding, an order shipping, a build finishing — eventually needs to
notify external systems over HTTP. This is "webhooks," and almost every
company that needs them builds their own version, once, under deadline
pressure, as a side project to whatever they were actually trying to
ship. The result is nearly always the same list of unglamorous bugs:

- A slow or dead customer endpoint blocks the request that triggered the
  event, or exhausts a thread pool, or both.
- Retries either don't exist (an event is lost forever if delivery fails
  once) or exist naively (hammering a dead endpoint every second forever).
- The same event gets delivered twice with no way for the receiver to
  know that, because there's no idempotency contract.
- Nobody can answer "did event X actually reach customer Y," because
  there's no durable log of attempts — only whatever's in application logs
  that rotated out three weeks ago.
- If webhook delivery is tied to billing (usage-based pricing, metered
  API calls), there's no way to prove the invoice is correct.

This is exactly the kind of infrastructure work engineers dread building
and companies dread having built badly: invisible when it works,
extremely visible (support tickets, lost revenue, angry customers) when
it doesn't.

## 2. Goal

Build a **reliable webhook delivery platform** that a real company could
put behind their event system on day one, and a **reference
implementation** of the backend design principles that make it reliable,
clear enough that another engineer can read the code and understand *why*
each decision was made, not just *what* the code does.

## 3. Users

- **Producers** (internal services at the company running Dispatcher):
  publish domain events ("order.created", "payment.failed") via the
  GraphQL API. They care about one thing — publishing an event must be
  fast and must never fail because a customer's endpoint is having a bad
  day.
- **Tenants / customers** (external companies receiving webhooks): register
  one or more endpoint URLs, subscribe to event types, and expect
  at-least-once delivery with a way to verify authenticity (HMAC
  signature) and de-duplicate (event ID).
- **Operators** (whoever runs Dispatcher): need to see the system's health
  at a glance, understand why a given delivery failed, and trust that
  usage numbers used for billing are correct.

## 4. Scope

### In scope (v1)
- Tenant, endpoint (CRUD), and event publishing via GraphQL.
- Transactional outbox: publishing an event and fanning it out to
  subscribed endpoints happens atomically.
- Idempotent event publishing via a client-supplied idempotency key.
- Asynchronous delivery with exponential backoff + jitter, up to a
  configurable max attempt count, then dead-lettering.
- Per-endpoint circuit breaking so a broken endpoint doesn't get hammered.
- HMAC-signed payloads so receivers can verify authenticity.
- Full attempt audit log (`delivery_attempt_logs`).
- Reconciliation between delivery outcomes and a downstream billing/usage
  ledger, on a schedule, with a persisted report.
- Archival of old attempt logs out of the operational table.
- Metrics (Prometheus) and structured logs for every stage of the
  pipeline; `/healthz` and `/readyz`.

### Explicitly out of scope (v1)
- A management UI (the user of this project said they'll handle frontend
  separately — the GraphQL API is the product surface for now).
- Multi-region / multi-cluster deployment topology.
- Payload transformation or filtering rules beyond "event type matches
  subscription."
- A hosted, multi-tenant SaaS control plane (auth beyond a basic API key
  per tenant is left as a documented extension point).
- Kafka/NATS-backed ingestion — the v1 queue is Postgres-native by design
  (see ARCHITECTURE.md for the tradeoff); swapping it in later is a
  documented extension, not a redesign.

## 5. Success criteria

A delivery is "reliable" here means, specifically and testably:

1. No event is ever silently dropped: every event that fans out to N
   endpoints ends in exactly one terminal state (`SUCCEEDED` or
   `DEAD_LETTERED`) per endpoint, and that transition is logged.
2. No event is ever delivered *fewer* times than it should be even under
   worker crashes (the stale-job reaper proves this).
3. A duplicate `publishEvent` call with the same idempotency key never
   creates a duplicate delivery.
4. A permanently-broken customer endpoint cannot degrade delivery to
   *other* customers' endpoints (proven by the circuit breaker being
   per-endpoint, not global, and by bounded HTTP timeouts).
5. The reconciliation job can always answer, for any time window,
   exactly which events were delivered but not billed, or billed but not
   delivered — zero silent drift.
6. The API and worker processes can each be scaled from 1 to N replicas
   with no code change and no coordination step.

## 6. Non-functional requirements

- **Latency:** `publishEvent` should return in low tens of milliseconds
  (bounded by one DB transaction), independent of subscriber endpoint
  health.
- **Durability:** once `publishEvent` returns success, the event and its
  fan-out targets exist durably in Postgres — a crash of every worker
  process immediately afterward loses zero deliveries.
- **Observability:** every failure mode described in this document must
  be visible in metrics or logs without reading source code to find it.
- **Portability:** runnable on a laptop via Docker Compose; no cloud
  vendor lock-in.

## 7. Risks / open questions

- The Postgres-native queue is simple and consistent but will not scale
  to extremely high ingestion volumes (tens of thousands of events/sec)
  without partitioning or moving ingestion to a log-based broker. Flagged
  in ARCHITECTURE.md as a deliberate v1 tradeoff, not an oversight.
- Multi-tenant auth is minimal (API key per tenant) — a real deployment
  needs rate limiting per tenant so one noisy tenant can't starve others'
  workers; noted as a next step in CURSOR_CONTEXT.md.
