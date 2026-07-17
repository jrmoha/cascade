# Read model — funnel (Postgres per-actor steps)

The Aggregator's third derived view (KAN-35): a **funnel** measuring how many actors
progress through an _ordered_ sequence of event types within a time window, served from
**PostgreSQL**. This is the relational "right tool" contrast to the Cassandra counters
([event-counts.md](event-counts.md)) and the Redis leaderboard
([leaderboard.md](leaderboard.md)) — funnels are slice-and-group questions Postgres
answers naturally. It is a worked example of the
[ADR-0015](../adr/0015-read-model-aggregation-strategy.md) strategy and the
[ADR-0017](../adr/0017-funnel-and-retention-views.md) decision.

> **Strategy vs. implementation.** The _why_ lives in
> [ADR-0017](../adr/0017-funnel-and-retention-views.md); this page is the _what_.

## What it serves

One Query API endpoint, read from Postgres (never raw Cassandra):

- `GET /funnel?projectId=&steps=&from=&to=` → per-step **actor counts** and cumulative
  **conversion rates** for the ordered `steps`, over the event-time window `[from, to]`.

> **Served from the read replica (KAN-41, [ADR-0019](../adr/0019-replication-and-consistency-model.md) §2).**
> This read routes to the Postgres streaming **read replica** (`DATABASE_REPLICA_URL`),
> which trails the primary by a bounded lag — _on top of_ the Aggregator's existing
> processing lag. Both are the same eventual-consistency guarantee this view already makes
> (there is no read-your-writes here). When no replica is configured, it falls back to the
> primary. See [`docs/runbooks/postgres-replication.md`](../runbooks/postgres-replication.md).

`steps` is a comma-separated list of **2–10 distinct** event types
(e.g. `?steps=game_start,level_complete,purchase`). `from`/`to` are ISO-8601 instants
bounding `occurredAt`. There is **no stored funnel object** — the step sequence is
supplied per request, so any funnel is ad-hoc (ADR-0017).

Response (`step` is 1-based; `conversionRate` is `actors / step-1 actors`, so step 1 = 1):

```json
{
  "projectId": "rpg",
  "from": "...",
  "to": "...",
  "steps": [
    { "step": 1, "eventType": "game_start", "actors": 4, "conversionRate": 1 },
    { "step": 2, "eventType": "level_complete", "actors": 2, "conversionRate": 0.5 },
    { "step": 3, "eventType": "purchase", "actors": 1, "conversionRate": 0.25 }
  ]
}
```

## Store & schema (Postgres)

A generic per-`(project, actor, eventType)` first-seen table — the Aggregator's **own**
table (raw `pg`, separate from the Project/Schema service's Prisma schema, ADR-0011),
created by [`0001_create_funnel_actor_steps.sql`](../../services/aggregator/migrations/postgres/0001_create_funnel_actor_steps.sql):

```sql
funnel_actor_steps (
  project_id    text,
  actor_id      text,
  event_type    text,
  first_seen_at timestamptz,        -- earliest occurredAt for this (actor, type)
  PRIMARY KEY (project_id, actor_id, event_type)
)
-- idx_funnel_steps_lookup (project_id, event_type, first_seen_at)
```

This **is** the per-actor state. "Furthest step reached" is not materialised; it is
derived at read time by pivoting this table per actor.

## What counts as an actor

The actor is resolved by the shared `actorKey(event)` helper in
[`libs/contracts/src/events.ts`](../../libs/contracts/src/events.ts): **`actorId`, falling
back to `sessionId`**. Events with neither are skipped (no error) — mirroring how the
leaderboard skips non-score events. The event time used is `occurredAt`.

## How the funnel is computed (read time)

For each actor, the Query API takes the earliest time each step's event type occurred
within the window (`t1..tn`). An actor counts at step _k_ only if it has `t1..tk` with
**non-decreasing** timestamps (`t2 ≥ t1, …, tk ≥ t(k-1)`) — i.e. it moved through the
steps **in order**, not merely performed them. The query builds the per-step pivot
dynamically from the requested `steps`
([`funnel.service.ts`](../../services/query-api/src/funnel/funnel.service.ts)).

## Idempotency — `LEAST` upsert (naturally idempotent)

The Aggregator upserts each step with
`first_seen_at = LEAST(existing, EXCLUDED)`. `MIN` over a set is commutative and
idempotent, so re-delivery (Kafka at-least-once) and a full replay from offset 0 converge
to the same earliest timestamp. Like the leaderboard's `ZADD GT`, this view needs **no
dedup gate of its own** (ADR-0016 §1); it rides the controller's shared per-`eventId`
gate as a harmless superset. The write runs in the valid-event branch in its own bounded
retry; a persistent failure is dead-lettered (ADR-0006).

## Rebuild

The table is a pure function of the log. To rebuild: `TRUNCATE funnel_actor_steps` and
replay `raw-events` from offset 0. Because the upsert is idempotent, **no dedup flush is
required** for this view (in contrast to the counters — ADR-0016 §3).

## Config

- `DATABASE_URL` (Aggregator, required) — writes the summary table.
- `DATABASE_URL` (Query API, required) — the Query API now reads this Postgres read model,
  so Postgres is one of its readiness deps. The Query API performs **no DDL/migrations**;
  the Aggregator owns the schema.

## Tests

- **Unit** —
  [`aggregator/test/funnel-retention.repository.spec.ts`](../../services/aggregator/test/funnel-retention.repository.spec.ts):
  actor-key resolution (actorId → sessionId → skip) and upsert params.
- **Integration** —
  [`aggregator/test/funnel-retention.e2e-spec.ts`](../../services/aggregator/test/funnel-retention.e2e-spec.ts)
  (real Kafka + Cassandra + Redis + Postgres): actor journeys produce the right
  `funnel_actor_steps` rows, `LEAST` keeps the earliest time under out-of-order/duplicate
  delivery, and an actorless event is ignored.
  [`query-api/test/funnel.postgres.e2e-spec.ts`](../../services/query-api/test/funnel.postgres.e2e-spec.ts)
  (real Postgres): ordered per-step conversion counts (incl. an out-of-order actor counted
  only at step 1), plus the `< 2 steps` and duplicate-steps `400`s.
