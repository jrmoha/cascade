-- KAN-35 — retention read model (ADR-0015 §2 / ADR-0017).
--
-- Per-actor active-day set: one row per (project, actor, UTC day) the actor was
-- active. The Aggregator's own relational read model (raw `pg`, separate from
-- Project/Schema's Prisma schema, ADR-0011) — NOT the raw write path. "Actor" is
-- actorId, falling back to sessionId (contracts.actorKey); the day is the UTC
-- calendar day of the event's occurredAt (event-time windowing, ADR-0015 §3).
--
-- One table covers the whole cohort matrix: a cohort is derived at read time as
-- each actor's earliest active day (MIN(active_period)), and retention at day
-- offset N counts distinct actors active on cohort_day + N. The Query API does
-- that group/self-join — the relational slicing Postgres is chosen for.
--
-- Idempotency (ADR-0016 §1): the writer inserts with ON CONFLICT DO NOTHING.
-- Set membership is idempotent and order-independent, and MIN(active_period) is
-- commutative, so re-delivery (Kafka at-least-once) and a full replay from
-- offset 0 reproduce the same rows with NO dedup gate — "naturally idempotent",
-- like the leaderboard's `ZADD GT`.

CREATE TABLE IF NOT EXISTS retention_actor_activity (
  project_id    text NOT NULL,
  actor_id      text NOT NULL,
  active_period date NOT NULL,            -- UTC calendar day of occurredAt
  PRIMARY KEY (project_id, actor_id, active_period)
);

-- Serves the cohort-range filter (cohort = MIN(active_period) per actor) and the
-- per-day distinct-actor counts the retention read groups by.
CREATE INDEX IF NOT EXISTS idx_retention_activity_lookup
  ON retention_actor_activity (project_id, active_period);
