# Cascade — Technical Blueprint

> This document is the engineering complement to [00-charter.md](00-charter.md). It describes the service topology, data flow, technology stack, and key design constraints. Major decisions are captured as ADRs in [adr/](adr/README.md).

---

## 1. System overview

Cascade separates the **write path** from the **read path** entirely. Events flow in through a thin collector, land on a durable queue, and are consumed independently by two processors. The Query API never touches raw data live — it reads only from pre-built aggregations.

```
                        ┌──────────────────────────────────┐
                        │           WRITE PATH             │
                        └──────────────────────────────────┘

 Clients ──► Collector ──► Kafka ──┬──► Ingestion-Processor ──► Cassandra (raw events)
                                   │
                                   └──► Aggregator ──► Redis (counters / leaderboards)
                                                   └──► PostgreSQL (funnel / retention views)

                        ┌──────────────────────────────────┐
                        │           READ PATH              │
                        └──────────────────────────────────┘

 Clients ──► Query API ──► Redis   (hot counters, leaderboards, rate-limits)
                       └──► PostgreSQL (aggregated views, project metadata)
```

---

## 2. Services

| Service                    | Responsibility                                                          | Language / Framework |
| -------------------------- | ----------------------------------------------------------------------- | -------------------- |
| **Collector**              | Accept inbound events over HTTP/gRPC, validate schema, publish to Kafka | NestJS (TypeScript)  |
| **Ingestion-Processor**    | Consume raw events from Kafka, write append-only to Cassandra           | NestJS (TypeScript)  |
| **Aggregator**             | Consume events from Kafka, update Redis counters and PostgreSQL views   | NestJS (TypeScript)  |
| **Query API**              | Serve pre-aggregated read models to clients                             | NestJS (TypeScript)  |
| **Project/Schema Service** | Manage projects, event schemas, API keys                                | NestJS (TypeScript)  |

Services communicate **asynchronously via Kafka** by default. Synchronous calls (gRPC or REST) are only used where a genuine request/response is required (e.g. schema validation lookup by the Collector).

---

## 3. Data stores

| Store          | Role                                                          | Why                                                                    |
| -------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------- |
| **Kafka**      | Durable event bus; decouples producers from consumers         | At-least-once delivery, replay, backpressure isolation                 |
| **Cassandra**  | Raw event storage; append-only, wide-row                      | Write-optimised, horizontally scalable, no joins needed for raw append |
| **Redis**      | Hot counters, leaderboards, rate-limit buckets                | Sub-millisecond reads; TTL-based eviction for time windows             |
| **PostgreSQL** | Project metadata, aggregated funnel/retention views, API keys | ACID, relational; low write volume; good for structured read models    |

See [adr/0001-overall-architecture.md](adr/0001-overall-architecture.md) for the rationale behind this topology.

---

## 4. Key design constraints

### Write path / read path separation

The Query API **never** scans raw Cassandra data live. All queries are served from Redis or PostgreSQL read models that the Aggregator maintains. If a feature seems to need a live raw scan, that is a design smell — stop and re-model.

### Cassandra: query-first modelling

Every Cassandra table is designed for a specific query. Partition keys include `project_id + time_window` to keep partitions bounded. No table is added without a stated query and partition strategy.

### Kafka consumers: idempotency

Both consumers (Ingestion-Processor and Aggregator) must be idempotent. Kafka delivers at-least-once; re-processing the same event must not double-count or duplicate rows. Deduplication is by event ID + time bucket.

### No new synchronous cross-service calls without justification

New sync calls require an explicit architectural reason. Default is async via Kafka.

---

## 5. Monorepo layout

```
services/
  collector/
  ingestion-processor/
  aggregator/
  query-api/
  project-schema/
libs/
  contracts/          shared event DTOs and Kafka topic definitions
  common/             shared utilities (logging, tracing, validation)
infra/
  docker-compose.yml  local dev environment
  terraform/          AWS infrastructure
docs/
  00-charter.md       product and learning goals
  blueprint.md        this file
  adr/                architecture decision records
  architecture/       diagrams and sequence flows
  contracts/          event schema definitions
  specs/              API specs (OpenAPI / AsyncAPI)
  runbooks/           operational procedures
```

---

## 6. Local development

All infrastructure dependencies (Kafka, Cassandra, PostgreSQL, Redis) run via Docker Compose. See `infra/docker-compose.yml` (to be added in KAN-14).

```bash
docker compose -f infra/docker-compose.yml up -d
npm install
# start individual services from their package directory
```

---

## 7. Phase gate

**Phase 0 — walking skeleton** (current): wire one event end-to-end through all five services with no business logic, prove the topology works. The end-to-end smoke test (KAN-20) must pass before Phase 1 work begins.

**Phase 1**: production-grade ingestion, schema validation, real aggregations.

**Phase 2**: load testing, chaos engineering, observability stack, AWS deployment.

---

## 8. Collector service (Phase 0 — KAN-17)

The first service of the write path. A thin NestJS app exposing a single endpoint:

- `POST /collect` — accepts an event, performs **light** validation (`projectId` and
  `type` required, non-empty strings; `occurredAt` and `payload` optional), enriches it
  into the canonical [`RawEvent`](contracts/events.md) envelope (stamping `eventId` and
  the ingest-time `receivedAt`), produces it to the Kafka `raw-events` topic, and returns
  `202 Accepted` with the stamped `eventId`.

Key decisions:

- **Kafka client:** `@nestjs/microservices` Kafka transport (`ClientKafka`) in
  producer-only mode. Brokers come from `KAFKA_BOOTSTRAP_SERVERS` (default
  `localhost:9092`).
- **Message key = `projectId`.** All events for a project land on the same partition,
  preserving per-project ordering and aligning with the Cassandra
  `(project_id, time_window)` partitioning.
- **`eventId` is server-stamped** (UUID v4) when absent. This is the stable idempotency
  key consumers use downstream for dedup (see ADR-0001 and the idempotency constraint
  above).

Light validation only here — real per-project schema validation and API-key checks are
Phase 1. The Collector owns no data store. Run/verify steps:
[runbooks/collector.md](runbooks/collector.md).

---

## 9. Ingestion-Processor service (Phase 0 — KAN-18)

The second write-path service. A NestJS **Kafka microservice** (consumer group
`cascade-ingestion-processor`) that consumes `raw-events` and appends each event to
Cassandra.

- **Consume:** `@nestjs/microservices` Kafka transport, `@EventPattern('raw-events')`.
- **Table:** `cascade.raw_events`, ensured on startup (`IF NOT EXISTS`). Query-first:
  partition key `(project_id, time_window)` (hourly UTC bucket), clustering key
  `event_id`. Serves `SELECT … WHERE project_id = ? AND time_window = ?`. See the
  [Cassandra mapping](contracts/events.md#cassandra-mapping-ingestion-processor).
- **Idempotency:** writes are primary-key upserts, so Kafka's at-least-once redelivery
  never duplicates rows — no separate dedup needed.

Minimal modelling only (one table); secondary query tables, TTLs, and prod topology are
Phase 1. Run/verify steps: [runbooks/ingestion-processor.md](runbooks/ingestion-processor.md).
