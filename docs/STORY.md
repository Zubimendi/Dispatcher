# The story behind Dispatcher

*Use this as a base for a LinkedIn/X post, a portfolio README blurb, or
talking points in an interview. Adjust the voice to sound like you, not
like a template.*

## The short version (for a post)

I kept seeing the same list of backend principles repeated in every
"senior engineer" thread — wrap multi-writes in transactions, make
retries idempotent, use circuit breakers, don't let caches go stale
quietly, reconcile anything involving money. Good advice, but advice you
only really internalize by hitting the failure mode it prevents.

So I picked a problem that forces almost all of them at once: reliable
webhook delivery. Every company that sends webhooks — Stripe, GitHub,
Shopify, and thousands of smaller SaaS products — ends up building some
version of this, usually badly, usually once, under deadline pressure.

I built **Dispatcher**: a Go + GraphQL service that accepts events,
durably records them in the same transaction as fanning them out to
subscriber endpoints, delivers over HTTP with retries, exponential
backoff and jitter, and per-endpoint circuit breaking, logs every attempt
for audit, and reconciles delivery outcomes against a downstream
billing ledger on a schedule so "what actually happened" always has an
answer.

It's open source. The part I'm actually proud of isn't the code — it's
[`docs/ARCHITECTURE.md`](ARCHITECTURE.md), which maps every one of those
20 principles to the exact file and function that implements it, and
explains the tradeoff behind each one. That doc is the artifact; the code
is the evidence it's not just talk.

Repo: https://github.com/Zubimendi/Dispatcher

## The longer version (for a blog post / README "why" section)

Most portfolio backend projects are CRUD apps with authentication bolted
on. They demonstrate that you can build an endpoint. They don't
demonstrate that you understand what happens when the downstream service
your endpoint depends on goes down at 2am, or when a client retries a
request that already succeeded, or when two workers try to update the
same row at the same time.

I wanted a project that couldn't be built correctly without confronting
those problems directly — not as an afterthought, not as a "we'll add
retries later," but as the actual shape of the design from the first
migration.

Webhook delivery turned out to be a near-perfect vehicle for that. It's a
real, well-understood problem (I'm not inventing a fake domain to justify
the architecture) that every backend engineer has either built or wished
someone else had built well for them. And it genuinely can't be built
*correctly* without:

- **Atomicity** — you cannot record "this event happened" and "here's who
  needs to hear about it" as two separate, non-transactional writes
  without risking a state where one exists and the other doesn't.
- **Idempotency** — HTTP is not exactly-once. Producers retry, consumers
  see duplicates. Pretending otherwise means either lost events or
  duplicate side effects, and in a system where someone might get billed
  based on delivery counts, duplicates cost real money.
- **Explicit state machines** — a delivery job has meaningfully different
  states (waiting, in flight, succeeded, exhausted-retries) and treating
  status as a loose string field instead of a governed set of transitions
  is exactly how you end up debugging "how did this get into an
  impossible state" at 2am.
- **Circuit breakers** — one customer's broken server should never be
  allowed to degrade delivery to every other customer, and it will,
  unless you design specifically to prevent it.
- **Reconciliation** — if delivery success feeds into billing, "probably
  consistent" isn't good enough. You need a job whose entire purpose is
  proving the two numbers match, because eventual consistency is a
  promise, not a guarantee, until something checks.

None of this is exotic. It's the boring, unglamorous 80% of what makes a
backend system trustworthy instead of merely functional — the stuff that
never shows up in a demo but is the entire difference between "runs on my
laptop" and "I can page someone about this at 3am and they'll know where
to look."

## Talking points for an interview

If asked "walk me through a project you're proud of":

1. **Start with the problem, not the tech.** "I noticed most backend
   portfolio projects don't force you to handle failure. I picked
   webhook delivery specifically because it's impossible to build
   correctly without transactions, idempotency, retries, and
   reconciliation all being load-bearing, not decorative."
2. **Pick one principle and go deep.** Good options: the transactional
   outbox pattern (`internal/outbox`), why the circuit breaker state
   lives in Postgres instead of worker memory, or the three separate
   layers of idempotency (producer, consumer, receiver contract) — each
   one closes a different duplication window.
3. **Be honest about the tradeoffs you made.** The Postgres-native queue
   instead of Kafka is the best example — say *why* (transactional
   consistency with the rest of the state) and *what it costs*
   (throughput ceiling), and that you'd revisit it at a different scale.
   Interviewers trust "here's what I traded off and why" far more than
   "this is the only correct way to do it."
4. **Point to the failure-mode tests, not just the happy path.** "I have
   a documented procedure for killing a worker mid-delivery and watching
   the stale-job reaper recover it" is a much stronger answer than "it
   has tests."
5. **Know what's not done.** Auth is minimal, there's no automated
   integration suite yet, tracing isn't wired up. Say so before they ask
   — it signals you know the difference between "portfolio-complete" and
   "production-hardened," which is itself the point of this project.

## Suggested post formats

**Short (X/LinkedIn):**
> Built a reliable webhook delivery platform in Go + GraphQL as a way to
> actually internalize backend reliability principles instead of just
> reading about them — transactional outbox, idempotent publishing,
> per-endpoint circuit breakers, exponential backoff with jitter, and a
> reconciliation job that proves delivery and billing numbers actually
> match. Open source, with a doc mapping every design decision to the
> failure mode it prevents: <link>

**Thread/carousel:** one principle per slide/tweet — problem it solves →
one sentence on the fix → link to the specific file. `ARCHITECTURE.md`
is written so each section can be lifted directly into a slide.
