-- Cascade raw event store (Phase 0 — KAN-18).
-- Source of truth for the keyspace + table the Ingestion-Processor ensures on
-- startup. Kept in sync with cassandra.service.ts. Proper modelling (secondary
-- query tables, TTLs, RF/topology) comes in Phase 1.

-- Local/dev only: SimpleStrategy RF=1. Replace with NetworkTopologyStrategy in prod.
CREATE KEYSPACE IF NOT EXISTS cascade
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Query this table serves:
--   SELECT * FROM cascade.raw_events WHERE project_id = ? AND time_window = ?;
-- Partition key (project_id, time_window) bounds partition size (hourly bucket);
-- clustering key event_id is unique within a partition and makes INSERT an
-- idempotent upsert (safe under Kafka at-least-once redelivery).
CREATE TABLE IF NOT EXISTS cascade.raw_events (
  project_id   text,
  time_window  text,        -- hourly bucket 'YYYY-MM-DDTHH' (UTC)
  event_id     uuid,
  type         text,
  event_time   timestamp,   -- original event timestamp
  payload      text,        -- JSON-encoded event payload
  PRIMARY KEY ((project_id, time_window), event_id)
);
