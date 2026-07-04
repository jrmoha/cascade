# 0017 — Funnel & retention derived views (Postgres, query-time funnel)

**Status:** Accepted

## Context

[ADR-0015](0015-read-model-aggregation-strategy.md) set the read-model strategy and
reserved **PostgreSQL summary tables** for the relational, slice-and-group views — funnels
and retention — while counters went to Cassandra (KAN-32) and the leaderboard to Redis
(KAN-34). [ADR-0016](0016-idempotent-replayable-aggregation.md) proved the
idempotency/replay contract and noted that, where a view permits it, we **prefer
naturally-idempotent operations** (best-score `ZADD GT`, upsert-by-key) over the per-
`eventId` dedup gate.

KAN-35 builds these last two Phase-1 views. Unlike counters and leaderboards they are
**stateful, per-actor** aggregations: a funnel needs each actor's progress through an
ordered step sequence; retention needs each actor's first-seen period and return activity.
This is the part of stream processing that motivates Flink/Kafka Streams; here it is done
by hand, which is the point (KAN-35 learning goal).

Two shaping questions had to be answered: **where the funnel step sequence lives**, and
**how to stay idempotent for per-actor state**.

## Decision

### 1. Actor identity is `actorId`, falling back to `sessionId`

Both views attribute events to an actor via a single shared helper,
`actorKey(event) = actorId ?? sessionId ?? null` (in `@cascade/contracts`, imported by both
writers so they can never disagree). Events with neither identifier are **skipped** (no
error) — the same "not relevant to this view" treatment the leaderboard gives events
without a `playerId`/`score`. Event time is `occurredAt` throughout (ADR-0015 §3).

### 2. Funnel: a generic per-actor step table, computed into a funnel at read time

There is **no stored funnel object** and no funnel-definition CRUD. The Aggregator
maintains a generic table `funnel_actor_steps(project_id, actor_id, event_type,
first_seen_at)` — the earliest event-time each actor performed each type. The Query API
computes an **ordered** funnel for an ad-hoc step list passed on the request
(`GET /funnel?steps=a,b,c&from=&to=`): it pivots the table per actor and counts an actor at
step _k_ only if it has `t1..tk` with non-decreasing timestamps. "Furthest step reached" is
thus _derived_, not materialised.

Rationale: this is maximally flexible (any funnel, no schema/management), keeps the writer
trivial, and is exactly the relational slicing Postgres is chosen for (ADR-0015 §2). The
per-actor step table **is** the stateful per-actor model the ticket calls for.

### 3. Retention: a per-actor active-day set, cohort derived at read time

The Aggregator maintains `retention_actor_activity(project_id, actor_id, active_period)` —
the set of UTC days each actor was active. One table covers the whole cohort matrix: a
cohort is **derived** at read time as each actor's earliest active day
(`MIN(active_period)`), and `GET /retention?from=&to=&maxOffset=` counts distinct returning
actors per day offset via a self-join. Granularity is the UTC day.

### 4. Both views are naturally idempotent — no dedup gate

Per ADR-0016 §1 we pick commutative, idempotent writes so neither view needs dedup state:

- **Funnel:** `INSERT … ON CONFLICT … DO UPDATE SET first_seen_at = LEAST(existing,
EXCLUDED)`. `MIN` over a set is commutative/idempotent.
- **Retention:** `INSERT … ON CONFLICT DO NOTHING` (set membership) + read-time `MIN`
  cohort (commutative).

Re-delivery (Kafka at-least-once) and a full replay from offset 0 converge to identical
rows. Rebuild is `TRUNCATE` + replay with **no dedup flush** (contrast the counters,
ADR-0016 §3). Each write runs in the controller's valid-event fan-out in its **own** bounded
retry, so a Postgres hiccup never re-runs the non-idempotent counter; a persistent failure
is dead-lettered (ADR-0006).

### 5. The Query API gains Postgres as a read dependency

The Query API now reads the Aggregator-owned funnel/retention tables, so it gets a **read-
only** `pg` pool (`DATABASE_URL`) and a Postgres readiness check — exactly how it already
treats Cassandra: it performs **no DDL or migrations** (the Aggregator owns the schema via
its SQL migrator). Postgres joins Cassandra and Redis as a Query API readiness dep. This is
a new data-store dependency, not a new cross-service sync call — no addition to the ADR-0009
sync-call inventory; the topic inventory is unchanged (the views are derived from the
existing `raw-events` stream).

## Consequences

- The Aggregator's empty `migrations/postgres/` (scaffolded in KAN-31) is now filled:
  `0001_create_funnel_actor_steps.sql`, `0002_create_retention_actor_activity.sql`.
- New ad-hoc-flexible funnel endpoint with no definition management; the cost is a small
  read-time pivot query (bounded by the 2–10 step cap) instead of O(1) counters — an
  acceptable trade for a low-write relational view (ADR-0015).
- New contracts `funnelResponse` / `retentionResponse` (Zod, snapshot-tested, additive —
  no `schemaVersion` bump) in `@cascade/contracts`.
- "Effectively-once" still holds: because both views are naturally idempotent, they do not
  even carry the one residual under-count edge the additive counters do (ADR-0016 §4).
- Proven in `aggregator/test/funnel-retention.e2e-spec.ts` (write path + replay/out-of-
  order) and `query-api/test/{funnel,retention}.postgres.e2e-spec.ts` (the conversion +
  cohort math). See `docs/read-models/funnel.md` and `docs/read-models/retention.md`.

This ADR instantiates [ADR-0015](0015-read-model-aggregation-strategy.md) §2 for the
Postgres views and applies [ADR-0016](0016-idempotent-replayable-aggregation.md) §1's
naturally-idempotent path; it refines neither.
