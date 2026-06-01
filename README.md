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
            each with its own Dockerfile, env contract, and health/readiness probes
libs/       Shared contracts, DTOs, and utilities
infra/      Terraform and docker-compose
docs/       Architecture decisions (ADRs) and design docs
```

## Prerequisites

- Node 22+ (LTS)
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

## Running the services

Each service is independently deployable — its own container, configured solely
via environment variables (validated by a Zod schema at boot), exposing its own
health and readiness endpoints. See [ADR-0010](docs/adr/0010-independently-deployable-services.md).

### Local (infra in Docker, services as processes)

```bash
make up                                    # start backing stores only (Kafka, Cassandra, …)
npm run start:dev -w @cascade/collector    # in separate terminals
npm run start:dev -w @cascade/ingestion-processor
npm run start:dev -w @cascade/query-api
```

### Full stack in Docker (the `apps` profile)

`make up` brings up only the backing stores. To build and run the three app
services as containers alongside them:

```bash
make stack-up      # docker compose --profile apps up -d --build
make stack-down
```

`make up` stays infra-only so the test/smoke workflow is unaffected.

### Configuration

Every variable is **required** unless noted, validated at boot — a missing or
invalid value fails fast. Each service documents its own variables in
`services/<service>/.env.example`. Infra/peer addresses use container service
names in Docker (`kafka:29092`, `cassandra`) and `localhost` from the host.

### Health & readiness

| Service             | Port | Liveness      | Readiness (deps)                 |
| ------------------- | ---- | ------------- | -------------------------------- |
| Collector           | 3001 | `GET /health` | `GET /ready` → Kafka             |
| Query API           | 3002 | `GET /health` | `GET /ready` → Cassandra         |
| Ingestion-Processor | 3003 | `GET /health` | `GET /ready` → Kafka + Cassandra |

Liveness = the process is up. Readiness = its dependencies are reachable (`/ready`
returns `503` when a dependency is down). The Ingestion-Processor is a hybrid app:
a Kafka consumer plus a small HTTP server that serves these probes.
