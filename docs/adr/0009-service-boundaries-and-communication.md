# 0009 — Service boundaries & communication strategy

**Status:** Accepted

## Context

[ADR-0001](0001-overall-architecture.md) chose a CQRS + event-driven microservice architecture and
named the services, but it argued the _shape_ of the system, not its seams. Before Phase 1 work
(KAN-27→30) starts wiring services together, the decomposition needs to be pinned down explicitly:
**one responsibility per service, every cross-service interaction classified and inventoried, and a
naming convention locked** — so later services follow one agreed contract instead of accreting
ad-hoc calls.

This ADR records those boundaries. It does **not** re-argue CQRS, Kafka, or the choice of Cassandra
— see ADR-0001 for that rationale. It is a decision record, not a design doc; the
[blueprint](../blueprint.md) and [charter](../00-charter.md) hold the detail.

Five services are in scope. Four exist in full today (**Collector**, **Ingestion-Processor**,
**Query API**, and **Project/Schema** as of KAN-28 — see
[ADR-0011](0011-project-schema-service.md)); the **Aggregator** exists as a **consumer skeleton** as
of KAN-31 ([ADR-0015](0015-read-model-aggregation-strategy.md)) — it consumes `raw-events` and
dead-letters, but derives no read models yet. The boundaries below hold for the skeleton and the
views that fill it in.

## Decision

### 1. One responsibility per service

Each service has **exactly one** primary responsibility and a set of stores it must not touch. A
service is the owner — and the only writer — of its store.

| Service                     | Single responsibility                                                           | Must not own / touch                 |
| --------------------------- | ------------------------------------------------------------------------------- | ------------------------------------ |
| **Collector**               | Validate events at the HTTP edge and produce them to Kafka                      | Any database                         |
| **Ingestion-Processor**     | Consume `raw-events` and persist them to Cassandra `raw_events` (append-only)   | Redis, PostgreSQL                    |
| **Aggregator** _(skeleton)_ | Consume `raw-events` and derive read models (counters/funnels/retention/boards) | The raw write path (`raw_events`)    |
| **Query API**               | Serve queries from read models (Redis/PostgreSQL); never aggregates over raw    | Kafka; raw Cassandra for aggregation |
| **Project/Schema**          | Own project metadata, schemas, and API keys in PostgreSQL                       | Kafka, Cassandra, Redis              |

Note on the Query API: per [ADR-0008](0008-raw-event-time-range-read.md) it may perform a **bounded,
partition-key-bounded raw _retrieval_** (`GET /query?projectId=&from=&to=`) for replay/audit, but it
**never aggregates** over raw Cassandra — analytics come exclusively from Aggregator read models.

Note on the Aggregator: [ADR-0015](0015-read-model-aggregation-strategy.md) refines its store rule.
It writes read models to **all three** stores — time-series counters to **Cassandra** (its own
_aggregate_ tables), leaderboards to **Redis**, funnel/retention summaries to **Postgres** (its own
tables, raw `pg`, separate from Project/Schema's Prisma schema). The invariant it must not cross is
the **raw write path**: it never reads or writes `raw_events`, and never aggregates over raw. This
broadens [ADR-0001](0001-overall-architecture.md)'s "Aggregator must not touch Cassandra" to "must
not touch the raw write path" — Aggregator-owned aggregate tables are fine.

**Boundary litmus test.** If two candidate services must always deploy together, or must share a
database table, they are not two services — they are one. Conversely, a single service that owns two
unrelated stores or two unrelated responsibilities should be split.

### 2. Communication strategy — async by default

Cross-service communication is **asynchronous via Kafka by default.** A producer emits an event and
moves on; consumers react independently with their own consumer groups. This keeps services
decoupled, absorbs bursts, and lets a slow or down consumer recover from the log without losing data
or stalling upstream.

A **synchronous** call (gRPC/REST) is introduced **only** when the caller genuinely cannot proceed
without an answer in the same request — e.g. "is this API key valid?" / "does this event match the
project's schema?" at ingest time. Every new sync dependency requires explicit justification in an
ADR; "it was easier" is not one. Async fan-out is never modelled as a chain of sync calls.

### 3. Topic inventory

Topic names are defined once in `@cascade/contracts` (`libs/contracts/src/events.ts`) and imported
everywhere — never re-typed as string literals.

| Topic            | Producer                        | Consumer(s)                                           | Partition key |
| ---------------- | ------------------------------- | ----------------------------------------------------- | ------------- |
| `raw-events`     | Collector                       | Ingestion-Processor; Aggregator _(skeleton)_          | `projectId`   |
| `raw-events.dlq` | Ingestion-Processor, Aggregator | Ad-hoc (inspection/replay tooling — no live consumer) | `projectId`   |

`projectId` as the partition key keeps a project's events ordered on one partition and co-locates
them for downstream processing (see [ADR-0002](0002-collector-kafka-production.md)). Dead-lettering
is defined in [ADR-0006](0006-dead-letter-handling.md). Each consumer uses its **own** consumer
group, so adding the Aggregator never disturbs the Ingestion-Processor.

### 4. Sync-call inventory

| Caller → Callee            | Purpose                                   | Transport | Status                                     |
| -------------------------- | ----------------------------------------- | --------- | ------------------------------------------ |
| Collector → Project/Schema | Validate API key / event schema at ingest | **gRPC**  | **Live** (callee KAN-28/29; caller KAN-30) |

This is now the **one live** synchronous service-to-service call. The Project/Schema **callee** exists
(KAN-28 / [ADR-0011](0011-project-schema-service.md)) and, as of KAN-29, the sync contract is a typed
**gRPC** service — `ProjectSchema.VerifyKey` / `GetEventSchema`, generated from
`libs/contracts/proto/project_schema.proto` ([ADR-0012](0012-inter-service-contract-versioning.md)) —
served by the hybrid (HTTP + gRPC) Project/Schema service alongside its REST admin endpoints. As of
**KAN-30** the Collector makes the **call** on its ingest hot path: it authenticates the `x-api-key`
header and validates each payload against the project's registered schema, **Redis-cached** and
**fail-closed**, before producing to Kafka ([ADR-0013](0013-collector-ingest-auth-validation-caching.md)).
It qualifies as the one justified sync dependency because the Collector cannot decide accept/reject
without an authoritative answer. Beyond it, all cross-service communication remains async via Kafka.

### 5. Topic naming convention

Locked now so it does not drift across services:

- **lowercase, hyphen-separated** words (`raw-events`, not `raw_events` or `rawEvents`);
- a **`.dlq`** suffix on the source topic name for its dead-letter queue (`raw-events.dlq`);
- defined as exported constants in `@cascade/contracts`, imported by producers and consumers alike.

### 6. Deployment & repo topology

- **Monorepo, npm workspaces:** `services/*` (one package per service), `libs/*` (shared contracts),
  and `e2e` (the smoke harness).
- **One deployable container per service.** Services share no in-process state; all coordination is
  via Kafka (async) or, where justified, a sync API.
- Infrastructure (Kafka, Cassandra, Redis, PostgreSQL) runs via `infra/docker-compose.yml` locally
  and is provisioned by Terraform in `infra/` for deployment.

## Alternatives considered

### A. Monolith with internal modules

One deployable; CQRS and module boundaries enforced in code rather than across the network.

**Rejected.** Lower operational overhead, but it fails the boundary goals: modules drift into shared
tables and shared transactions, and the charter (§3) explicitly targets independently deployable
services with their own stores. This mirrors ADR-0001's rejected alternative D.

### B. Sync-everywhere (gRPC/REST between all services)

Model every interaction as a request/response call — Collector calls Ingestion-Processor, which
calls the Aggregator, etc.

**Rejected.** It couples availability (any callee down fails the caller), removes Kafka's buffering,
replay, and independent fan-out, and pushes back-pressure all the way to the client during ingest
spikes. The write path must absorb bursts without blocking — a synchronous chain cannot.

## Consequences

**Positive:**

- Boundaries are explicit and testable against the litmus test; planned services slot into an agreed
  topology rather than improvising.
- Async-by-default keeps services decoupled and the write path burst-tolerant; consumers evolve and
  recover independently via the log.
- The topic and sync-call inventories are the canonical reference for KAN-27→30 — a new interaction
  is "real" only once it appears in a table here.

**Trade-offs:**

- Operational overhead of five services plus four backing stores (see ADR-0001 for the full list).
- Eventual consistency between ingest and read models is inherent to the async fan-out.
- Idempotency is mandatory: at-least-once delivery means every consumer must deduplicate.
- No cross-service joins; data is denormalised in write models or fanned out in the Query API.
