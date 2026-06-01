# 0010 — Independently deployable services: containers, config & health

**Status:** Accepted

## Context

[ADR-0001](0001-overall-architecture.md) and [ADR-0009](0009-service-boundaries-and-communication.md)
decided that Cascade is a set of independently deployable services. Until now that was only true on
paper: the three built services (Collector, Ingestion-Processor, Query API) shared a single
`tsc --build`, had **no Dockerfiles**, were **absent from `docker-compose`** (only the backing stores
ran there), read config through scattered inline defaults (`process.env.X ?? 'localhost'`) with **no
boot-time validation**, and exposed **no health/readiness endpoints**. The Ingestion-Processor was a
pure Kafka microservice with no HTTP port at all.

KAN-27 makes independent deployability real: each service must build, run, and fail on its own.

## Decision

### 1. One container per service

Each service gets a multi-stage `Dockerfile` (build → slim runtime, no dev dependencies). Because the
monorepo uses npm workspaces + TS project references and every service depends on `@cascade/contracts`,
the **build context is the repo root**; the builder runs `npm ci`, builds contracts + the one service,
then `npm prune --omit=dev`; the runtime stage copies the pruned `node_modules` plus the built `dist`
of contracts and the service. Shared code stays **only** in `libs/` — no service imports another's
source (already true; now enforced by the per-service build boundary).

### 2. Config solely via env, validated with Zod at boot

Every service owns a per-service **Zod** env schema (`src/config/env.schema.ts`) and a `@Global()`
config module that loads `.env` (local dev; absent in containers, where compose injects env) and
parses the merged environment **once at boot**, exposing a frozen, fully-typed `APP_CONFIG`. A
missing/invalid var throws and the process exits before serving traffic (12-factor fail-fast). **All
inline defaults are removed**: infra/peer addresses (`KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_*`) are
**required** and come from config using container service names (`kafka:29092`, `cassandra`) — never
hardcoded. Only a service's own HTTP bind `PORT` keeps a conventional default (it is not a peer
address).

### 3. Liveness + readiness via `@nestjs/terminus`

Each service exposes `GET /health` (liveness — the process is up) and `GET /ready` (readiness — its
dependencies are reachable): Kafka for the Collector, Cassandra for the Query API, both for the
Ingestion-Processor. Readiness uses terminus' Kafka microservice ping and a small custom Cassandra
indicator (`SELECT now() FROM system.local`). Phase 5 (k8s) will consume both probes.

### 4. Ingestion-Processor becomes a hybrid app

To serve HTTP health probes while remaining a Kafka consumer, the processor bootstraps as
`NestFactory.create` (HTTP) + `connectMicroservice` (Kafka) + `startAllMicroservices`. The
`@EventPattern` handler and the `cascade-ingestion-processor` consumer group are unchanged.

### 5. docker-compose `apps` profile

The three services are added to `infra/docker-compose.yml` under `profiles: [apps]`, so `make up`
stays infra-only (preserving the test/smoke workflow) and `make stack-up` runs the full stack. Each
service is independent: health-gated `depends_on`, its own container, and stopping one does not stop
the others.

## Alternatives considered

- **Keep the Ingestion-Processor a pure microservice and expose readiness over a non-HTTP channel.**
  Rejected: probes are consumed by container/k8s HTTP checks; a hybrid app is the idiomatic Nest way
  and keeps the probe contract uniform across all three services.
- **A shared `@cascade/cassandra-client` / `@cascade/kafka-client` lib to remove duplication.**
  Deferred (out of scope): the per-service client setup is small, and duplication here reinforces the
  independence the ticket is about. Revisit if the duplication grows.
- **Hand-rolled health controllers instead of terminus.** Rejected: terminus provides the aggregation
  and indicator plumbing (and a Kafka indicator) for a small, idiomatic dependency.

## Consequences

- Each service builds and ships as its own slim image and runs independently; a misconfig fails fast
  at startup with a clear Zod error rather than at first request.
- The processor now also binds an HTTP port (default 3003) purely for probes.
- **Removing config defaults shifts responsibility to the environment**: every runtime (compose, the
  root `.env`, and test harnesses) must supply the required infra vars. Integration/smoke harnesses
  already set them; this is now mandatory, not optional.
- Operational surface grows (Dockerfiles, an `apps` compose profile) but stays a single compose file.
