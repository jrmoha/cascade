# Read model — retention (Postgres per-actor active days)

The Aggregator's fourth derived view (KAN-35): a **retention cohort matrix** measuring,
for each cohort (actors first seen on a UTC day), how many return on subsequent days,
served from **PostgreSQL**. Like the funnel ([funnel.md](funnel.md)) it is the relational
"right tool" view — a cohort triangle is a group/self-join. It is a worked example of the
[ADR-0015](../adr/0015-read-model-aggregation-strategy.md) strategy and the
[ADR-0017](../adr/0017-funnel-and-retention-views.md) decision.

> **Strategy vs. implementation.** The _why_ lives in
> [ADR-0017](../adr/0017-funnel-and-retention-views.md); this page is the _what_.

## What it serves

One Query API endpoint, read from Postgres (never raw Cassandra):

- `GET /retention?projectId=&from=&to=&maxOffset=` → the **cohort matrix** for cohorts
  (actors first seen on a UTC day) within `[from, to]`, up to `maxOffset` days after each
  cohort day.

`from`/`to` are UTC calendar days (`YYYY-MM-DD`); `maxOffset` defaults to 7 (max 90), and
the cohort range is capped at 92 days. Granularity is the UTC **day**.

Response (offset 0 is the cohort size; offsets only appear where there was activity):

```json
{
  "projectId": "rpg",
  "granularity": "day",
  "cohorts": [
    {
      "cohort": "2026-05-01",
      "cohortSize": 3,
      "offsets": [
        { "offset": 0, "actors": 3 },
        { "offset": 1, "actors": 1 },
        { "offset": 2, "actors": 2 }
      ]
    },
    {
      "cohort": "2026-05-02",
      "cohortSize": 1,
      "offsets": [
        { "offset": 0, "actors": 1 },
        { "offset": 1, "actors": 1 }
      ]
    }
  ]
}
```

## Store & schema (Postgres)

A per-actor active-day **set** — the Aggregator's **own** table (raw `pg`, separate from
the Project/Schema service's Prisma schema, ADR-0011), created by
[`0002_create_retention_actor_activity.sql`](../../services/aggregator/migrations/postgres/0002_create_retention_actor_activity.sql):

```sql
retention_actor_activity (
  project_id    text,
  actor_id      text,
  active_period date,               -- UTC calendar day of occurredAt
  PRIMARY KEY (project_id, actor_id, active_period)
)
-- idx_retention_activity_lookup (project_id, active_period)
```

One table covers the whole matrix: a cohort is **derived at read time** as each actor's
earliest active day (`MIN(active_period)`); there is no separate cohort-assignment table.

## What counts as an actor

Resolved by the shared `actorKey(event)` helper in
[`libs/contracts/src/events.ts`](../../libs/contracts/src/events.ts): **`actorId`, falling
back to `sessionId`**; events with neither are skipped. The active day is the UTC calendar
day of `occurredAt`, via the shared `dailyLeaderboardPeriod()` helper (the same one the
daily leaderboard uses), so a late event lands on the day it happened (ADR-0015 §3).

## How retention is computed (read time)

Each actor's cohort = `MIN(active_period)`. Retention at day offset _N_ counts the distinct
actors of that cohort active on `cohort_day + N` (offset 0 = cohort size). A self-join over
the activity table groups by `(cohort, active_period − cohort)`
([`retention.service.ts`](../../services/query-api/src/retention/retention.service.ts)).

## Idempotency — set insert (naturally idempotent)

The Aggregator inserts each active day with `INSERT … ON CONFLICT DO NOTHING`. Set
membership is idempotent and order-independent, and the read-time cohort `MIN` is
commutative, so re-delivery and a full replay from offset 0 reproduce the same rows. Like
the funnel, this view needs **no dedup gate** (ADR-0016 §1); the write runs in the
valid-event branch in its own bounded retry, dead-lettered on persistent failure.

## Rebuild

`TRUNCATE retention_actor_activity` and replay `raw-events` from offset 0. The insert is
idempotent, so **no dedup flush is required** (ADR-0016 §3).

## Config

- `DATABASE_URL` (Aggregator, required) — writes the summary table.
- `DATABASE_URL` (Query API, required) — the Query API reads this read model (Postgres is a
  readiness dep); it performs no DDL/migrations (the Aggregator owns the schema).

## Tests

- **Unit** —
  [`aggregator/test/funnel-retention.repository.spec.ts`](../../services/aggregator/test/funnel-retention.repository.spec.ts):
  actor-key resolution and UTC-day derivation of the insert params.
- **Integration** —
  [`aggregator/test/funnel-retention.e2e-spec.ts`](../../services/aggregator/test/funnel-retention.e2e-spec.ts)
  (real Kafka + Cassandra + Redis + Postgres): actor activity produces the right
  `retention_actor_activity` rows under duplicate/out-of-order delivery.
  [`query-api/test/retention.postgres.e2e-spec.ts`](../../services/query-api/test/retention.postgres.e2e-spec.ts)
  (real Postgres): the cohort matrix (cohort = earliest active day, returners per offset),
  plus the `from > to` and malformed-date `400`s.
