# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Cascade. Each ADR captures a significant architectural decision: the context, the options considered, the choice made, and the consequences.

## Format

ADRs follow the [MADR](https://adr.github.io/madr/) lightweight template:

- **Status**: Proposed | Accepted | Deprecated | Superseded by [XXXX]
- **Context**: What situation forced this decision?
- **Decision**: What was chosen?
- **Consequences**: What are the trade-offs?

## Index

| #                                                        | Title                                                                                        | Status               |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------- | -------------------- |
| [0001](0001-overall-architecture.md)                     | Overall system architecture (CQRS + Kafka + Cassandra + microservices)                       | Accepted             |
| [0002](0002-collector-kafka-production.md)               | Collector Kafka production strategy (transport, message key, eventId)                        | Accepted             |
| [0003](0003-query-api-phase0-raw-read.md)                | Query API Phase-0 raw read-back from Cassandra (temporary)                                   | Superseded by [0008] |
| [0004](0004-canonical-event-contract.md)                 | Canonical event contract (Zod single-source envelope; event/ingest time)                     | Accepted             |
| [0005](0005-validate-at-collector-edge.md)               | Validate at the Collector edge with the shared contract                                      | Accepted             |
| [0006](0006-dead-letter-handling.md)                     | Dead-letter handling for failed events (DLQ + bounded retry)                                 | Accepted             |
| [0007](0007-cassandra-raw-events-model.md)               | Cassandra `raw_events` query-first model (partition key, TTL, migrations)                    | Accepted             |
| [0008](0008-raw-event-time-range-read.md)                | Bounded raw event-retrieval read (time-range `GET /query`, pagination)                       | Accepted             |
| [0009](0009-service-boundaries-and-communication.md)     | Service boundaries & communication strategy (topic + sync-call inventories)                  | Accepted             |
| [0010](0010-independently-deployable-services.md)        | Independently deployable services (containers, Zod config, health probes)                    | Accepted             |
| [0011](0011-project-schema-service.md)                   | Project/Schema service (Postgres via Prisma, hashed API keys, JSON schemas)                  | Accepted             |
| [0012](0012-inter-service-contract-versioning.md)        | Inter-service contracts & versioning (gRPC `.proto`, versioned Kafka schema, CI enforcement) | Accepted             |
| [0013](0013-collector-ingest-auth-validation-caching.md) | Collector ingest auth, per-project schema validation & caching (fail-closed, Redis)          | Accepted             |
| [0014](0014-nestjs-11-prisma-7-upgrade.md)               | Upgrade to NestJS 11 + Prisma 7 (Postgres via `@prisma/adapter-pg`, `prisma.config.ts`)      | Accepted             |
| [0015](0015-read-model-aggregation-strategy.md)          | Read-model & aggregation strategy (CQRS): views/stores, event-time windowing, dedup, rebuild | Accepted             |
| [0016](0016-idempotent-replayable-aggregation.md)        | Idempotent & replayable aggregation (the keystone): dedup key, offset commit, rebuild, proof | Accepted             |
| [0017](0017-funnel-and-retention-views.md)               | Funnel & retention derived views (Postgres; query-time funnel, naturally-idempotent upserts) | Accepted             |
| [0018](0018-enforce-cqrs-read-boundary.md)               | Enforce the CQRS read boundary; counts read endpoint (arch-test guardrail, bounded fan-out)  | Accepted             |

## Creating a new ADR

Copy the template below, name the file `NNNN-short-title.md`, and add a row to the index above.

```markdown
# NNNN — Title

**Status:** Proposed

## Context

...

## Decision

...

## Consequences

...
```
