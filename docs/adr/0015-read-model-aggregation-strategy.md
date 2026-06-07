# 0015 — Read-model & aggregation strategy (CQRS)

**Status:** Accepted

> Jira KAN-31 titled this "ADR-0004"; that number was already taken by
> [ADR-0004](0004-canonical-event-contract.md). This is the next free number.

## Context

[ADR-0001](0001-overall-architecture.md) made CQRS — strict separation of the write and read paths —
"the single most important constraint in the system", and reserved an **Aggregator** that derives
read models from the event log. [ADR-0008](0008-raw-event-time-range-read.md) drew the real boundary
between **retrieval** (bounded raw reads for replay/audit) and **aggregation** (counters, funnels,
retention, leaderboards), and stated that aggregation is served **exclusively** from Aggregator read
models — never by scanning raw Cassandra. [ADR-0009](0009-service-boundaries-and-communication.md)
placed the Aggregator in the topology as a second, independent consumer of `raw-events` and marked it
_planned_. [ADR-0004](0004-canonical-event-contract.md) split `occurredAt` (event time) from
`receivedAt` (ingest time) precisely so aggregation could key off **when an event happened**.

That is the skeleton. Before building the Aggregator, the read-model approach must be **decided and
recorded** — which derived views exist, which store each lives in, how they window time, how they stay
correct under at-least-once delivery, and how they are rebuilt — so each view lands in the right store
and is reproducible from the log, instead of query hacks accreting later. This ADR records those
decisions. It is the strategy; the views themselves are built in follow-up tickets.

## Decision

### 1. The CQRS boundary is absolute and one-directional

The **write path** (Collector → Kafka → Ingestion-Processor → Cassandra `raw_events`) and the **read
path** (Aggregator → read models → Query API) share no store and no code path:

- The **Aggregator is the only writer of read models.** Nothing else mutates them.
- **Analytics reads never touch raw storage.** The Query API serves counters/funnels/retention/boards
  **only** from read models. Its one raw capability is the bounded, partition-key-bounded **retrieval**
  of [ADR-0008](0008-raw-event-time-range-read.md) (`GET /query`) for replay/audit — never aggregation.
- The two sides are **eventually consistent** by construction (Kafka sits between them). We embrace
  that, not fight it: a read model lags the log by the Aggregator's processing latency.

If a feature appears to need a live aggregation over raw rows, that is a missing read model — add it to
the Aggregator, do not scan raw ([ADR-0001](0001-overall-architecture.md)).

### 2. Derived views, the read each serves, and its store

Three stores are used **deliberately**, each matched to an access pattern:

| View                                       | Read it serves                                          | Store                         | Why this store                                                                                           |
| ------------------------------------------ | ------------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------------------- |
| **Event counts** per minute / per hour     | time-series counts for `(projectId, type)` over a range | **Cassandra** aggregate table | high write volume, append-mostly, range-by-bucket; same query-first/partition discipline as `raw_events` |
| **Live leaderboard** (top-N)               | top-N for a `(projectId, board)`                        | **Redis** sorted set          | sub-millisecond ranked top-N via `ZREVRANGE`                                                             |
| **Funnel** (ordered step conversion)       | per-cohort step counts                                  | **Postgres** summary table    | relational, sliced/grouped/joined                                                                        |
| **Retention** (cohort returns over N days) | cohort × day-offset grid                                | **Postgres** summary table    | relational, sliced/grouped/joined                                                                        |

Picking by access pattern (time-series → Cassandra, ranked top-N → Redis ZSET, relational summaries you
slice/group → Postgres) is intentional — we use all three so the differences are felt.

This **refines** the earlier store rule. [ADR-0001](0001-overall-architecture.md) and the blueprint
gave the Aggregator only Redis + Postgres and told it not to touch Cassandra (counters were sketched
into Redis). Putting time-series counters in Cassandra instead — so all three stores are exercised on
their best-fit access pattern — means the Aggregator now owns its **own aggregate tables** in Cassandra.
The real invariant is unchanged and re-stated sharply: the Aggregator never touches the **raw write
path** (`raw_events` / the Ingestion-Processor's domain), and the Query API never aggregates over raw.
"Must not touch Cassandra" becomes "must not touch the raw write path".

The Aggregator **owns its Postgres tables** in the shared Postgres instance, accessed with the raw `pg`
driver and a small filesystem SQL migrator (the same shape as the Ingestion-Processor's Cassandra
`Migrator`). It does **not** go through the Project/Schema service or its Prisma layer — Prisma stays
fenced to that one bounded context ([ADR-0011](0011-project-schema-service.md)), and "drivers are used
raw everywhere else" holds. Postgres simply gains a second, independent owner with its own tables.

### 3. Windowing: event-time, tumbling, with a bounded lateness horizon

- **Window on event time (`occurredAt`), not processing time.** This is _why_
  [ADR-0004](0004-canonical-event-contract.md) separated event time from ingest time: late and
  out-of-order events are normal in telemetry, and an event must land in the window for **when it
  happened**. Counters reuse the hourly bucket helpers in `libs/contracts/src/time-window.ts`
  (`toHourlyBucket`, `hourlyBucketRange`); a minute-bucket helper is added with the counts view.
- **Tumbling (non-overlapping) windows** are the baseline: 1-minute and 1-hour for counts, daily
  cohorts for retention. Sliding/session windows are a later refinement, not part of this strategy.
- **Late-event policy.** An event is admitted to its event-time window if it arrives within a bounded
  **lateness horizon**; because every view keys off event time and is idempotent (§4), a late event
  simply applies to the correct past window with no special casing. The horizon is bounded by the
  view's retention and ultimately by the raw-events 30-day TTL
  ([ADR-0007](0007-cassandra-raw-events-model.md)); events older than the horizon are dropped (and the
  log can always be replayed to recompute history exactly — §5).

### 4. Idempotency under at-least-once: dedup by `eventId`, prefer idempotent ops

Kafka is at-least-once, so the Aggregator can see the same event (same `eventId`) more than once. Naive
additive updates (a counter `+1`, a Redis `ZINCRBY`) would double-count, which
[ADR-0006](0006-dead-letter-handling.md) / the project's idempotency rule forbid. Strategy:

- **`eventId` is the idempotency key** (it already is downstream — the Cassandra clustering key in
  [ADR-0007](0007-cassandra-raw-events-model.md)).
- **Prefer naturally-idempotent operations** where the view allows it: a "best score" leaderboard uses
  `ZADD GT` (a set/max, replay-safe), an upsert-by-key summary overwrites rather than accumulates.
- For genuinely **additive** views (per-window counts, total-score boards), **dedup by `eventId` over a
  bounded horizon** before applying the increment — a small dedup store (e.g. a Redis key with TTL, or
  a Cassandra table) sized to the lateness horizon. A redelivery within the horizon is a no-op.
- **Not Kafka exactly-once (EOS).** EOS only makes Kafka→Kafka state atomic; our sinks (Cassandra,
  Redis, Postgres) are **external**, so EOS would not cover them and app-level idempotency would still
  be required. The dedup approach is simpler and actually covers the sinks we have.

> The concrete dedup gate, the offset-commit guarantee, the accepted residual crash edge, and the
> executable proof of these properties are pinned down in
> [ADR-0016](0016-idempotent-replayable-aggregation.md) (the keystone).

### 5. Rebuildability: replay `raw-events` from offset 0

The log is the source of truth; every read model is a **pure, deterministic function of the events**.
Any view — wrong, lost, or newly added — is regenerated by **replaying `raw-events` from offset 0** into
clean view tables. Determinism holds because views key off event time and the stable `eventId`, so a
replay reproduces them exactly. The procedure: truncate the target view store(s) → run the Aggregator
as a **rebuild consumer** (a fresh consumer group, or offsets reset to earliest) → let it catch up. This
is the payoff of CQRS-over-a-log: adding a new analytic is "design the view, replay", not a backfill
migration. The rebuild must also **flush the dedup state** so the replay dedups against itself rather
than the original pass's expired markers ([ADR-0016](0016-idempotent-replayable-aggregation.md) §3).

### 6. The Aggregator is a second, independent consumer

It consumes `raw-events` in its **own** consumer group (`cascade-aggregator`, parallel to the
Ingestion-Processor's `cascade-ingestion-processor`), so the two never disturb each other
([ADR-0009](0009-service-boundaries-and-communication.md)). Like the Ingestion-Processor it is
**idempotent** and **never throws out of the handler**: a message that fails validation is routed to
`raw-events.dlq` immediately, and a valid event whose view-write keeps failing is dead-lettered after a
bounded retry ([ADR-0006](0006-dead-letter-handling.md)).

## Alternatives considered

- **Live aggregation over raw Cassandra.** Rejected — it violates the core CQRS constraint
  ([ADR-0001](0001-overall-architecture.md)); a query scanning millions of raw rows cannot meet the
  read-path latency bar. Aggregations must be materialised ahead of time.
- **One store for all views.** Rejected — a single store cannot be simultaneously the best fit for
  time-series counters, ranked top-N, and relational summaries. The point is to match store to access
  pattern.
- **Processing-time windows.** Rejected — keying off arrival time mis-buckets late/out-of-order
  telemetry; event-time is the whole reason [ADR-0004](0004-canonical-event-contract.md) split the two
  timestamps.
- **Kafka exactly-once (EOS).** Rejected — does not cover the external Cassandra/Redis/Postgres sinks,
  adds significant operational complexity, and still needs app-level idempotency.
- **Approximate-live, rebuild-only correctness** (no live dedup; reconcile via periodic full rebuild).
  Considered as the simpler hot path, but it lets live counters be transiently over-counted, which the
  "never double-count" rule disallows. Dedup-by-`eventId` was chosen; the rebuild story remains the
  backstop for correctness and for adding new views.
- **Aggregator writing summaries through Project/Schema's Prisma.** Rejected — it breaks the ADR-0011
  fence and couples two bounded contexts. The Aggregator owns its own raw-`pg` tables instead.

## Consequences

**Positive:**

- The read-model approach is fixed before any view is coded: each derived view has a justified store, a
  windowing rule, an idempotency strategy, and a rebuild path.
- Full **rebuildability** from the log — new analytics are a replay, not a backfill.
- The Aggregator drops into the agreed ADR-0009 topology as an independent consumer; the write path is
  untouched.

**Trade-offs:**

- **Eventual consistency** between write and read sides is now explicit and must be reasoned about in UI
  and tests.
- Additive views pay a **per-event dedup** cost (an extra lookup/write) and carry a bounded dedup store.
- **Three stores to operate** for read models, plus Postgres gaining a second owner — the Aggregator's
  tables are kept separate from Project/Schema's Prisma-managed ones, by convention not by enforcement.
- A correct rebuild requires the log to still hold the needed history (bounded by the raw-events 30-day
  TTL); older history is gone once the buffer rolls.

## Implementation

The first view realizing this strategy is **event counts** (KAN-32): per-minute and per-hour Cassandra
`counter` tables, windowed on `occurredAt`, made replay-safe by a per-`eventId` Redis dedup gate before
each increment. See [`docs/read-models/event-counts.md`](../read-models/event-counts.md). Leaderboards,
funnels, and retention are follow-up tickets that reuse the same consumer, windowing, and idempotency
machinery.
