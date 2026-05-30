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
