-- KAN-32 — windowed event-count read models (ADR-0015 §2).
--
-- Two Cassandra `counter` aggregate tables: per-minute and per-hour event counts
-- keyed by (project, eventType, time-bucket), windowed by EVENT TIME (occurredAt).
-- These are the Aggregator's own read-model tables; they are NOT the raw write
-- path (raw_events) and never touched by the Ingestion-Processor.
--
-- Partition discipline mirrors raw_events (ADR-0007): partition key
-- ((project_id, time_bucket)) bounds partition size to one project + one
-- time-unit, and clustering on event_type lets a (project, type)-over-range read
-- enumerate buckets with single-partition SELECTs (like hourlyBucketRange does
-- for raw retrieval) — never a cross-partition scan.
--
-- Counter caveats (why dedup, not the row key, enforces idempotency): a `counter`
-- column cannot be part of the PRIMARY KEY and a table holding one may hold no
-- other non-PK columns, so event_id cannot live here. `count = count + 1` is also
-- NOT idempotent under Kafka at-least-once redelivery. The Aggregator therefore
-- dedups by eventId (Redis SET NX, sized to the lateness horizon) BEFORE applying
-- the increment (ADR-0015 §4); a redelivery within the horizon is a no-op. The
-- view stays fully rebuildable by replaying raw-events from offset 0 (ADR-0015 §5).

CREATE TABLE IF NOT EXISTS cascade.event_counts_by_minute (
  project_id  text,
  time_bucket text,        -- 'YYYY-MM-DDTHH:MM' minute bucket of occurredAt (UTC)
  event_type  text,
  count       counter,
  PRIMARY KEY ((project_id, time_bucket), event_type)
);

CREATE TABLE IF NOT EXISTS cascade.event_counts_by_hour (
  project_id  text,
  time_bucket text,        -- 'YYYY-MM-DDTHH' hour bucket of occurredAt (UTC)
  event_type  text,
  count       counter,
  PRIMARY KEY ((project_id, time_bucket), event_type)
);
