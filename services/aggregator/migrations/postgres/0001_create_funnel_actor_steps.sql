-- KAN-35 — funnel read model (ADR-0015 §2 / ADR-0017).
--
-- Per-actor step-completion state: the earliest event-time at which each actor
-- performed each event type, per project. This is the Aggregator's own
-- relational read model (raw `pg`, separate from the Project/Schema service's
-- Prisma schema, ADR-0011) — NOT the raw write path.
--
-- There is no predefined "funnel" object: the Query API computes an ordered
-- funnel ad-hoc from a step list + window over this generic per-actor table
-- (group/pivot per actor — exactly the relational slicing Postgres is chosen for,
-- ADR-0015). "Actor" is actorId, falling back to sessionId (contracts.actorKey).
--
-- Idempotency (ADR-0016 §1): the writer upserts with
-- `first_seen_at = LEAST(existing, new)`. MIN over a set is commutative and
-- idempotent, so re-delivery (Kafka at-least-once) and a full replay from
-- offset 0 converge to the same earliest timestamp with NO dedup gate — the same
-- "naturally idempotent" property the leaderboard gets from `ZADD GT`.
--
-- PRIMARY KEY (project_id, actor_id, event_type) makes the upsert a single-row
-- ON CONFLICT. The secondary index serves the funnel read's window filter on a
-- given (project, event_type).

CREATE TABLE IF NOT EXISTS funnel_actor_steps (
  project_id    text        NOT NULL,
  actor_id      text        NOT NULL,
  event_type    text        NOT NULL,
  first_seen_at timestamptz NOT NULL,
  PRIMARY KEY (project_id, actor_id, event_type)
);

CREATE INDEX IF NOT EXISTS idx_funnel_steps_lookup
  ON funnel_actor_steps (project_id, event_type, first_seen_at);
