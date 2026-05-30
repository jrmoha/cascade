# 0001 — Overall system architecture

**Status:** Accepted

## Context

Cascade ingests high-volume behavioral events and serves real-time aggregations (counters, funnels,
retention cohorts, leaderboards). The workload has two sides with fundamentally opposite
characteristics:

**Write side** — high-volume, bursty, append-only. A player session or match can produce hundreds
of events per second. Ingestion must absorb spikes without back-pressure reaching the client. Write
latency tolerance is loose (sub-second acceptable). Durability is non-negotiable; losing events is
worse than slowing down.

**Read side** — dashboard and API queries expect pre-shaped, low-latency answers (target: <50 ms
p95). A live query that scans millions of raw rows cannot meet this bar. Aggregations must be
materialized ahead of time.

These two requirements are in direct tension: optimizing storage for fast ingest (append-only,
wide-row) makes ad-hoc reads expensive, and optimizing for fast reads (indexed, relational)
introduces write contention at scale.

A further constraint: the system must be horizontally scalable and resilient to single-node failure
on both sides independently.

## Decision

Adopt a **CQRS + event-driven microservice** architecture composed of four interrelated choices:

---

### 1. CQRS — strict separation of write and read paths

The write path and read path share no data store and no code path.

- **Write path**: Collector → Kafka → Ingestion-Processor → Cassandra
- **Read path**: Aggregator (Kafka consumer) → Redis / PostgreSQL ← Query API

The Query API has zero visibility into Cassandra. If a proposed feature requires the Query API to
read raw event data, that is a design error — the aggregation is missing and must be added to the
Aggregator instead.

This is the single most important constraint in the system. It is the reason the read path can be
fast even under heavy write load.

---

### 2. Kafka as the durable backbone

All events flow through Kafka after the Collector accepts them. No service writes directly to
another service's store. Kafka provides:

- **Durability** before downstream processing — a consumer crash does not lose events.
- **Replay** — a consumer group can be reset to reprocess history without re-ingesting from
  clients.
- **Fan-out** — Ingestion-Processor and Aggregator consume the same topics independently, with
  independent consumer groups, without coupling.
- **Back-pressure isolation** — a slow Aggregator does not stall the Collector or
  Ingestion-Processor.

At-least-once delivery is accepted; all consumers **must** be idempotent.

---

### 3. Cassandra as the write-optimised raw event store

Raw events are stored append-only in Cassandra. Table design is query-first:

- Partition key: `(project_id, time_window)` — bounds partition size, enables time-range scans per
  project.
- Clustering key: `event_id` — unique within a partition, supports idempotent upsert.

Cassandra is chosen over alternatives because:

| Concern                              | Cassandra                            | PostgreSQL                  | DynamoDB                 |
| ------------------------------------ | ------------------------------------ | --------------------------- | ------------------------ |
| Write throughput at scale            | Excellent — LSM-tree, no write locks | Limited — B-tree, row locks | Good, but vendor lock-in |
| Horizontal scale-out                 | Peer-to-peer, no single master       | Requires sharding proxy     | Managed but opaque       |
| Schema flexibility for event payload | Wide rows, JSON column for payload   | JSONB works but heavier     | Flexible but costly      |
| Self-hosted                          | Yes                                  | Yes                         | No                       |

No ad-hoc relational queries are run against Cassandra. Any query that cannot be answered by a
pre-defined partition key scan is redirected to the read models.

---

### 4. Small, independently deployable microservices

Five services, each with a single bounded responsibility:

| Service                 | Owns                             | Does not touch          |
| ----------------------- | -------------------------------- | ----------------------- |
| **Collector**           | Kafka (producer)                 | Any database            |
| **Ingestion-Processor** | Cassandra                        | Redis, PostgreSQL       |
| **Aggregator**          | Redis, PostgreSQL (write models) | Cassandra               |
| **Query API**           | Redis, PostgreSQL (read models)  | Cassandra, Kafka        |
| **Project/Schema**      | PostgreSQL (metadata)            | Kafka, Cassandra, Redis |

Cross-service communication is **async via Kafka by default**. Synchronous calls (gRPC or HTTP) are
only introduced where a genuine request/response semantic is required (e.g. the Collector asking
the Project/Schema service to validate a schema at ingest time). Each new sync dependency requires
explicit justification.

Services are deployed as separate containers and share no in-process state.

---

## Alternatives considered

### A. Single PostgreSQL database

Write events to PostgreSQL; aggregate with background jobs or triggers.

**Rejected** because: PostgreSQL write throughput degrades under the target ingest rate; live
aggregation queries over millions of rows are too slow for the read SLA; the single store becomes a
bottleneck and a single point of failure.

### B. Synchronous write-through with an in-process cache

Collector writes to Cassandra directly; an in-process aggregation cache is warmed on each write.

**Rejected** because: the Collector becomes the aggregation engine, coupling ingestion and
computation. A burst of writes starves cache maintenance. Cache state is lost on restart with no
replay mechanism. Horizontal scaling the Collector requires distributed cache coordination.

### C. Polling-based aggregation (batch recompute)

A scheduled job reads recent raw events and recomputes aggregation tables periodically.

**Rejected** because: the read SLA requires near-real-time aggregations; polling adds latency equal
to the polling interval. The job re-reads data that was already processed, wasting I/O. Recovery
from a failed run requires careful bookmarking logic.

### D. Monolith with separated internal modules

Single deployable; CQRS enforced at the module boundary in code, not at the service boundary.

**Considered seriously**. Lower operational overhead, simpler local development, easier
transactions. Rejected because the learning goal stated in [00-charter.md](../00-charter.md) §3
explicitly targets independently deployable services with their own data stores. The monolith path
does not develop that competency.

---

## Consequences

**Positive:**

- Write and read paths scale independently; a spike in ingestion does not affect query latency.
- Kafka enables replay and decoupled consumer evolution.
- Cassandra handles append-heavy write volume without contention.
- Each service is small, focused, and independently deployable.
- Failure in one consumer (e.g. Aggregator) does not drop events — Kafka retains them until the
  consumer recovers.

**Negative / trade-offs:**

- **Operational complexity**: five services plus Kafka, Cassandra, Redis, and PostgreSQL must all
  run locally (Docker Compose) and in production.
- **Eventual consistency**: there is a small, observable lag between an event being ingested and
  its aggregation appearing in the Query API. This is acceptable for analytics dashboards; it would
  not be acceptable for financial ledgers.
- **Idempotency is mandatory and non-trivial**: every consumer must deduplicate on event ID to
  handle Kafka's at-least-once delivery. This must be tested explicitly.
- **No joins across services**: data that would be trivially joined in a relational monolith
  requires either denormalisation in the write model or a fan-out read in the Query API.

The complexity is intentional and consistent with the learning objectives of the project (see
[00-charter.md](../00-charter.md) §3).
