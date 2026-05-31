-- KAN-24: query-first raw_events model. See ADR-0007.
--
-- Primary read served: "all events for a project within a time range, newest first."
--
-- Partition key (project_id, time_bucket): time_bucket is the hourly UTC bucket
-- 'YYYY-MM-DDTHH' derived from occurred_at (event time). Bucketing bounds each
-- partition to one project-hour, so a hot project can't grow an unbounded
-- partition (the trap of partitioning by project_id alone).
--
-- Clustering (occurred_at DESC, event_id ASC): rows come back newest-first
-- straight from Cassandra (no app-side sort) and support time-range slices
-- within a bucket; event_id breaks ties and guarantees uniqueness, keeping the
-- write an idempotent upsert under Kafka at-least-once redelivery.
--
-- default_time_to_live = 2592000 (30 days): raw events are the replay/audit
-- buffer; the durable answers live in the Aggregator read models (ADR-0001).
-- 30 days bounds storage while leaving a generous replay window.
CREATE TABLE cascade.raw_events (
  project_id   text,
  time_bucket  text,
  occurred_at  timestamp,
  event_id     uuid,
  type         text,
  received_at  timestamp,
  payload      text,
  session_id   text,
  actor_id     text,
  source       text,
  PRIMARY KEY ((project_id, time_bucket), occurred_at, event_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)
  AND default_time_to_live = 2592000;
