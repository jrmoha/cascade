# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Cascade. Each ADR captures a significant architectural decision: the context, the options considered, the choice made, and the consequences.

## Format

ADRs follow the [MADR](https://adr.github.io/madr/) lightweight template:

- **Status**: Proposed | Accepted | Deprecated | Superseded by [XXXX]
- **Context**: What situation forced this decision?
- **Decision**: What was chosen?
- **Consequences**: What are the trade-offs?

## Index

| #                                          | Title                                                                     | Status               |
| ------------------------------------------ | ------------------------------------------------------------------------- | -------------------- |
| [0001](0001-overall-architecture.md)       | Overall system architecture (CQRS + Kafka + Cassandra + microservices)    | Accepted             |
| [0002](0002-collector-kafka-production.md) | Collector Kafka production strategy (transport, message key, eventId)     | Accepted             |
| [0003](0003-query-api-phase0-raw-read.md)  | Query API Phase-0 raw read-back from Cassandra (temporary)                | Accepted (temporary) |
| [0004](0004-canonical-event-contract.md)   | Canonical event contract (Zod single-source envelope; event/ingest time)  | Accepted             |
| [0005](0005-validate-at-collector-edge.md) | Validate at the Collector edge with the shared contract                   | Accepted             |
| [0006](0006-dead-letter-handling.md)       | Dead-letter handling for failed events (DLQ + bounded retry)              | Accepted             |
| [0007](0007-cassandra-raw-events-model.md) | Cassandra `raw_events` query-first model (partition key, TTL, migrations) | Accepted             |

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
