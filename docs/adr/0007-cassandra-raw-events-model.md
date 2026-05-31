# 0007 — Cassandra `raw_events` query-first data model

**Status:** Accepted

> The KAN-24 ticket calls this "ADR-0002"; that number was already taken (Collector Kafka
> production), so the partition-key decision is recorded here as ADR-0007.

## Context

`raw_events` is the write-path store consumed back by the read path. Cassandra rewards modelling
around the **query you must serve**, not what's convenient to write. The primary read is:

> _"all events for a project within a time range, newest first."_

Two things had to be decided properly for Phase 1:

1. **The partition key** — the single most consequential Cassandra decision. Partitioning by
   `project_id` alone gives one ever-growing partition per project; a busy project's partition grows
   without bound and eventually destroys read/compaction performance. The partition must be bounded.
2. **How the schema is created** — Phase 0 created the table inline at app startup
   (`CREATE TABLE IF NOT EXISTS`). That isn't a versioned, repeatable migration and can't evolve the
   schema over time.

## Decision

```cql
CREATE TABLE cascade.raw_events (
  project_id text, time_bucket text, occurred_at timestamp, event_id uuid,
  type text, received_at timestamp, payload text,
  session_id text, actor_id text, source text,
  PRIMARY KEY ((project_id, time_bucket), occurred_at, event_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)
  AND default_time_to_live = 2592000;
```

1. **Partition key `(project_id, time_bucket)`.** `time_bucket` is the **hourly** UTC bucket
   `YYYY-MM-DDTHH`, derived from `occurred_at` (event time). This bounds every partition to a single
   project-hour, so no project can grow an unbounded partition. Hourly (vs daily) keeps partitions
   small for high-volume game telemetry / hot projects; the cost is that a multi-hour read fans out
   to a few single-partition queries, which is cheap and stays partition-key-bounded.

2. **Clustering `(occurred_at DESC, event_id ASC)`.** Rows come back **newest-first directly from
   Cassandra** (no application-side sort) and the model supports time-range slices within a bucket
   (`occurred_at >= ? AND occurred_at <= ?`). `event_id` breaks `occurred_at` ties, guarantees row
   uniqueness, and keeps the write an **idempotent upsert** under Kafka at-least-once redelivery.

3. **TTL `default_time_to_live = 2592000` (30 days).** Raw events are the replay/audit buffer; the
   durable answers live in the Aggregator read models (ADR-0001). 30 days bounds storage while
   leaving a generous replay window for reprocessing or backfills.

4. **Versioned migrations replace startup DDL.** A small in-repo runner (`Migrator`) applies the
   committed `services/ingestion-processor/migrations/*.cql` files exactly once each, tracking
   applied ids in a `schema_migrations` table. It runs on startup _and_ via
   `npm run migrate -w @cascade/ingestion-processor`. The `.cql` files are the single source of
   truth; nothing is created ad-hoc via `cqlsh`.

## Alternatives considered

- **Partition by `project_id` alone:** the unbounded-partition trap — rejected outright.
- **Daily `time_bucket`:** fewer partitions to scan for wide ranges, but partitions grow ~24× larger
  and risk hot/large partitions for busy projects. Rejected for Phase 1; revisit per-project if a
  project is low-volume.
- **`event_id` as the sole clustering column (the Phase-0 model):** required an app-side sort and
  offered no time-range slicing. Replaced.
- **A third-party migration library:** the only Node option targeting cassandra-driver v4
  (`cassandra-migration`) is CLI-only (calls `process.exit`), unmaintained (2022, CoffeeScript), and
  would force every integration test to run a migrate step out-of-process. The lightweight in-repo
  runner integrates cleanly (startup + CLI) with no dependency. Rejected.
- **`ALLOW FILTERING`:** never used. Every read is bounded by the full partition key.

## Consequences

- Reads are newest-first with no app-side sort and never need `ALLOW FILTERING`.
- This is a breaking key change from the Phase-0 table. `CREATE TABLE` (not `IF NOT EXISTS`) in the
  migration makes a conflicting pre-existing local table fail loudly; recreate dev data once with
  `docker compose -f infra/docker-compose.yml down -v` (RF=1 throwaway data). Tests use ephemeral
  containers and are unaffected.
- Migrations live in the `ingestion-processor` package (it owns the schema, per ADR-0003) and are
  resolved relative to the module, so they apply under `dist`, `ts-node`, and Vitest regardless of
  CWD. A deployment that ships only `dist` must also ship the `migrations/` directory.
- Keyspace creation remains bootstrap (SimpleStrategy RF=1, local only); real environments provision
  the keyspace with NetworkTopologyStrategy out-of-band.
