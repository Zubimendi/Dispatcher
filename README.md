# Dispatcher

A reliable webhook delivery platform, built in Go with a GraphQL API, as a
reference implementation of production backend design principles — not a
toy CRUD app with GraphQL bolted on.

Every company that sends webhooks (Stripe, GitHub, Shopify, and thousands
of smaller SaaS products) ends up building some version of this: accept an
event, fan it out to customer-configured URLs, retry the ones that fail,
don't melt down when a customer's server is broken, and be able to prove
what was actually delivered. Most companies build it badly, once, under
deadline pressure, and live with the consequences. This is what building
it *well* looks like.

## What it does

- Tenants register **endpoints** (a URL + which event types they care about).
- Producers **publish events** through the GraphQL API.
- Dispatcher durably records the event and fans it out to every matching
  endpoint, then delivers each one over HTTP with a signed payload.
- Failed deliveries retry with exponential backoff and jitter; endpoints
  that are consistently broken get automatically circuit-broken so they
  stop being hammered.
- Every attempt is logged. A reconciliation job proves that "delivered"
  and "billed" numbers actually match.

## Why it's structured this way

Read [`ARCHITECTURE.md`](ARCHITECTURE.md) — it maps every
principle in the brief this project was built from to the specific file
and function that implements it. That document is the actual deliverable;
the code is the evidence.

## Quickstart

Requires Go 1.22+, Docker, and Docker Compose.

```bash
git clone https://github.com/Zubimendi/Dispatcher dispatcher && cd dispatcher
cp .env.example .env
go mod tidy              # resolves dependencies (needs network access)
make up                  # starts Postgres + Redis + a webhook-sink, runs migrations
make api                 # terminal 1: GraphQL server on :8080/graphql
make worker               # terminal 2: delivery workers
make reconciler           # terminal 3 (optional, run on a schedule): reconciliation + archival
```

Open `http://localhost:8080/graphql` for the interactive GraphiQL explorer.

Minimal end-to-end flow:

```graphql
mutation {
  createEndpoint(
    tenantId: "00000000-0000-0000-0000-000000000001"
    url: "http://webhook-sink:9000/"
    eventTypes: ["order.created"]
  ) { id url circuitState }
}

mutation {
  publishEvent(
    tenantId: "00000000-0000-0000-0000-000000000001"
    eventType: "order.created"
    payload: "{\"orderId\": \"abc123\"}"
    idempotencyKey: "order-abc123-created"
  ) { id status }
}
```

You'll need a `tenants` row first — see `docs/TESTING.md` for a seed
script. Watch `docker compose logs -f webhook-sink` to see the delivery
arrive, signed and all.

## Repo layout

```
cmd/api          GraphQL HTTP server (stateless, horizontally scalable)
cmd/worker       Delivery worker pool (stateless, horizontally scalable)
cmd/reconciler   Scheduled reconciliation + archival job
internal/        All business logic, organized by concern (see ARCHITECTURE.md)
pkg/models       Shared domain types
```


## License

MIT — see `LICENSE`.
