# Cascade

Real-time event analytics platform — self-hostable, event-driven, built for high-volume game telemetry.

## Documentation

- [Project Charter](docs/00-charter.md) — problem, goals, scope, and technology rationale
- [Blueprint](docs/blueprint.md) — service architecture, data flow, and design constraints
- [ADR index](docs/adr/README.md) — architecture decision records
- [CLAUDE.md](CLAUDE.md) — working agreement, architecture rules, and conventions

## Structure

```
services/   NestJS microservices (Collector, Ingestion-Processor, Aggregator, Query API, Project/Schema)
libs/       Shared contracts, DTOs, and utilities
infra/      Terraform and docker-compose
docs/       Architecture decisions (ADRs) and design docs
```

## Prerequisites

- Node 20+
- npm 10+

## Getting started

```bash
npm install
npm run lint
npm run format:check
```

## End-to-end smoke test (the walking-skeleton gate)

`KAN-20` proves the whole pipe with a single command: an event POSTed to the
Collector flows **in → Kafka → Cassandra → queried back out** of the Query API,
and the read-back is asserted to match what went in.

```bash
npm run smoke
```

This is a self-contained integration test (`e2e/test/smoke.e2e-spec.ts`). It uses
[Testcontainers](https://testcontainers.com/) to start a real Kafka and a real
Cassandra, boots all three services in-process against them
(Collector, Ingestion-Processor, Query API), then `POST /collect` → polls
`GET /query` until the event lands and asserts the envelope round-trips exactly.

- **Requires Docker** to be running (it pulls `confluentinc/cp-kafka` and
  `cassandra:4.1` on first run, so the first run is slow).
- Where Docker is unavailable, skip it with `SKIP_INTEGRATION=1 npm run smoke`
  (same flag honoured by the per-service integration tests).

Passing this gate is the precondition for starting Phase 1 (see
[CLAUDE.md](CLAUDE.md) → Phase gate).
