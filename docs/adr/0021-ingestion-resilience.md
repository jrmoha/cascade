# 0021 — Ingestion resilience: rate limiting, backpressure, produce retries & circuit breaking

**Status:** Accepted

## Context

Phase 4 (epic KAN-9) hardens Cascade for scale and failure. The replication pieces landed first
(Cassandra multi-node KAN-38/39, Postgres primary + replica KAN-41). **KAN-42 hardens the write
edge — the Collector — so a traffic spike or a slow dependency degrades gracefully instead of
falling over or losing accepted data.**

Before this ticket the Collector had no edge protection. `CollectorService.collect()` awaited a
single Kafka `emit` with no bound on concurrent produces and no retry; there was no rate limiting;
and `ProjectSchemaClient` called Project/Schema over gRPC with a 5 s timeout and the ADR-0013
fail-closed/cached policy but **no circuit breaker**, so every call to a dead peer waited the full
timeout. The downstream consumer already handles transient failures with bounded retry + DLQ and a
retriable/non-retriable split ([ADR-0006](0006-dead-letter-handling.md), KAN-23) — that half was
done; this ADR is about the producer edge.

The guiding principle: **an unbounded buffer just relocates the failure to OOM.** Under overload we
bound the work and shed the excess with an explicit status code — we never silently drop data a
client believes was accepted, and we never block the write path on a slow dependency.

## Decision

Four mechanisms on the Collector, all config-driven (per-service Zod env, ADR-0010) with sensible
non-infra defaults.

### 1. Per-API-key rate limiting → `429`

A Redis **token bucket**, one bucket per API key (keyed by the **SHA-256** of the key, so no
plaintext secret lands in Redis — same hygiene as the ingest cache). Refill-and-consume is a single
Lua `EVAL`, so concurrent requests for a key cannot race the check-then-decrement. The bucket
refills at `RATE_LIMIT_REFILL_PER_SEC` up to `RATE_LIMIT_BURST`; a drained bucket yields
`429 Too Many Requests` with a `Retry-After` header. `RateLimitGuard` runs **before** `ApiKeyGuard`,
so a flood on one key is capped before it reaches auth or Project/Schema. The limiter **fails open**
(a Redis error allows the request): it is a spike shield, not a security boundary — auth still fails
closed (ADR-0013). Redis is already a Collector readiness dep, so no new dependency. Per-key (not
per-project) was chosen so one noisy key is throttled without penalising a tenant's other keys.

### 2. Backpressure → bounded in-flight, `503`

A non-blocking counting semaphore (`InFlightLimiter`) caps concurrent produces at
`PRODUCE_MAX_INFLIGHT`. When full, `collect()` sheds immediately with `503` rather than queueing
unboundedly. There is deliberately **no** waiting queue — shedding fast is the point, and the client
holds the un-acknowledged event.

### 3. Produce retry with backoff → `503` on exhaustion

A transient produce is retried up to `PRODUCE_MAX_ATTEMPTS` with exponential backoff
(`PRODUCE_RETRY_BASE_MS * 2^(n-1)`), each attempt bounded by `PRODUCE_TIMEOUT_MS` (an rxjs `timeout`
that also cancels the underlying send). If every attempt fails, `collect()` returns `503` — the
event was **never acknowledged** to the client (never a `202`), so a retry is safe and nothing is
dropped. There is **no Collector-side DLQ**: a dead Kafka cannot receive one, and the honest signal
is "we could not accept this; try again." This mirrors the processor's retry shape (KAN-23) on the
producer side; the envelope build (a pure function) is not retried — a failure there is a `500`.

### 4. Circuit breaker around the sync dependency → fast fail-closed

The two Project/Schema gRPC calls (`VerifyKey`, `GetEventSchema`) are wrapped in an `opossum`
breaker (one per method). The Redis cache check runs **in front of** the breaker, so only genuine
RPCs are guarded and a warm hit never touches it. The breaker trips when the error rate exceeds
`PROJECT_SCHEMA_BREAKER_ERROR_PCT` over at least `PROJECT_SCHEMA_BREAKER_VOLUME` calls and stays open
for `PROJECT_SCHEMA_BREAKER_RESET_MS`. While open, `.fire()` rejects **instantly** — no per-request
5 s hang — and the caller falls back to exactly the KAN-30 policy: a warm cache hit is served, a cold
miss is a fast `503` (fail-closed). Critically, an **`errorFilter` excludes gRPC `NOT_FOUND`**: an
unregistered schema is _data_ (→ `422`), not an outage, and must never trip the breaker.

### 5. Load test as a CI gate (AC evidence)

A k6 spike (`infra/load/ingest-spike.js`) drives `POST /collect` and asserts the service stays
responsive (p95 threshold) while treating `429`/`503` as _expected_ graceful shedding rather than
failures. Afterwards `reconcile.mjs` sums the `raw-events` end offsets and asserts **produced ≥
accepted (202)** — proving no accepted data is lost (a surplus is duplicate-from-retry, acceptable
under at-least-once since downstream is idempotent, ADR-0016). It runs against a **trimmed**
single-node stack (`infra/load/docker-compose.load.yml` — just Collector + Kafka + Redis +
Project/Schema + Postgres) in a dedicated `load-test` CI job and via `make load-test`.

## Alternatives considered

- **`@nestjs/throttler` (+ a Redis storage adapter)** for rate limiting. Rejected: adds a dependency
  whose Redis storage is third-party and departs from the raw-driver convention used everywhere else;
  a ~20-line Lua token bucket over the Redis client we already have is simpler and fully owned.
- **A tiered `raw-events.retry` topic** for AC#3. Rejected as over-building for a learning project:
  it means a new topic, a new consumer, and an ADR-0009 inventory change, for no benefit the
  producer-retry → `503` (client retries) + the existing consumer DLQ don't already give.
- **Manual/`make`-only load test** (KAN-39/41 demo-script precedent). Considered, but the ticket
  wants the spike to be a real gate, so it is wired into CI (heavier, accepted) — with `make
load-test` kept for local runs.
- **Synchronous produce with no in-flight bound** (rely on KafkaJS's internal queue). Rejected: that
  queue is effectively unbounded under a sustained spike — the OOM failure mode this ticket exists to
  prevent.

## Consequences

**Positive:**

- The write edge degrades gracefully: a spike is shed as `429`/`503`, never OOM and never silent
  data loss; the k6 gate proves accepted events all reach Kafka.
- A Project/Schema outage no longer costs a 5 s hang per cold request — the breaker fails fast into
  the same fail-closed/cached policy already in place.
- Every mechanism is a tunable env knob, defaulted, documented in `.env.example`, and unit-tested.

**Trade-offs:**

- Under aggressive limits some legitimate requests get `429`/`503` and must be retried by the client
  (the deliberate load-shedding cost); tuning the knobs is a per-deployment concern.
- The breaker can, for one `RESET_MS` window, fail cold requests closed even if Project/Schema has
  just recovered (a half-open trial call re-closes it) — the standard breaker trade.
- The `load-test` CI job builds two service images and runs a stack, so it is materially slower than
  `verify`; it is a separate job so it never slows the fast checks.
- No Collector-side durability for un-acknowledged events: if Kafka is down past the retries, the
  client owns the retry. This is intentional — the Collector stays stateless.
