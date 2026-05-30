# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Cascade. Each ADR captures a significant architectural decision: the context, the options considered, the choice made, and the consequences.

## Format

ADRs follow the [MADR](https://adr.github.io/madr/) lightweight template:

- **Status**: Proposed | Accepted | Deprecated | Superseded by [XXXX]
- **Context**: What situation forced this decision?
- **Decision**: What was chosen?
- **Consequences**: What are the trade-offs?

## Index

| #                                    | Title                                                                  | Status   |
| ------------------------------------ | ---------------------------------------------------------------------- | -------- |
| [0001](0001-overall-architecture.md) | Overall system architecture (CQRS + Kafka + Cassandra + microservices) | Accepted |

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
