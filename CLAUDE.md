# CLAUDE.md — Cascade

Real-time event analytics platform. This file is the brief; the full reasoning lives in `docs/00-charter.md`, `docs/blueprint.md`, and `docs/adr/`. Read those before non-trivial work.

## What this is

A self-hostable engine that ingests high-volume events, stores them raw, computes real-time aggregations (counters, funnels, retention, leaderboards), and serves them. Demo domain: game telemetry. Backend + deployment + scaling only — no frontend.

## Architecture (see docs/adr/0001)

Event-driven. Collector → **Kafka** → two independent consumers: Ingestion-Processor (writes raw to **Cassandra**) and Aggregator (derives read models). Query API serves **only** from read models. **PostgreSQL** holds metadata; **Redis** holds hot counters/leaderboards/rate-limits. Services: Collector, Ingestion-Processor, Aggregator, Query API, Project/Schema.

## Non-negotiable rules

- **Write path and read path are separate; analytics never scans raw.** Counters, funnels, retention and leaderboards are served **only** from Aggregator read models (Redis/Postgres) — never by aggregating over raw Cassandra. If a feature needs to _aggregate_ over raw rows live, that's a design smell; stop and flag it. **Bounded raw _retrieval_** is allowed and supported: the Query API's `GET /query?projectId=&from=&to=` reads raw events for replay/audit/debugging, but only ever **partition-key-bounded** (per-`(project_id, time_bucket)` single-partition slices, span-capped) — never a cross-partition scan, never `ALLOW FILTERING`. See ADR-0008 (supersedes the Phase-0 ADR-0003) and `docs/contracts/events.md` → "Reading events back".
- **Cassandra is modeled query-first.** Never add a table without stating the exact query it serves and its partition key. Watch partition size; bucket by `project_id + time_bucket` (hourly). Schema changes go through the versioned migrations in `services/ingestion-processor/migrations/` (applied by `Migrator` on startup and `npm run migrate`) — never ad-hoc `cqlsh` or inline `CREATE TABLE`. See ADR-0007 for the `raw_events` model.
- **Postgres (Project/Schema service) is the one ORM exception — Prisma, fenced to persistence.** The relational config store (projects, API keys, event schemas) uses **Prisma** over Postgres; everywhere else drivers are used raw. Prisma owns only the **DB schema** (`services/project-schema/prisma/schema.prisma`) via **versioned migrations** under `prisma/migrations/` — applied with `prisma migrate deploy` on service boot and `npm run migrate`, never `db push`/`synchronize`. The **wire/API contracts stay Zod** in `@cascade/contracts` (Prisma must not leak into the cross-service surface). API keys are stored **argon2-hashed** with a unique-indexed non-secret `prefix` for one-lookup verification, and are revocable; event schemas are JSON Schema in a `jsonb` column. See ADR-0011 and `docs/contracts/project-schema.md`.
- **All Kafka consumers must be idempotent.** Delivery is at-least-once. Re-processing the same message must not double-count. Consumers must not throw out of the handler (a poison message would loop forever and block its partition): route failures to the dead-letter topic instead — validation failures immediately, transient (persistence) failures after a bounded retry. See ADR-0006 / `docs/runbooks/dlq.md`.
- **No new cross-service synchronous call without justification.** Prefer events. Sync calls (gRPC/REST) only for genuine request/response needs. Service boundaries (one responsibility each), the topic inventory, and the sync-call inventory are recorded in ADR-0009 — a new cross-service interaction is "real" only once it's listed there. **Kafka topics are lowercase-hyphen with a `.dlq` suffix for dead letters** (`raw-events`, `raw-events.dlq`), defined as constants in `@cascade/contracts` and imported — never re-typed as string literals.

## Stack & conventions

- NestJS + TypeScript. Node 22+ (LTS). (Pin versions in package.json.)
- Lint/format: ESLint + Prettier. Commits: Conventional Commits (Commitlint + Husky).
- Tests: Vitest (unit), Testcontainers (integration against real Cassandra/Kafka/Postgres). Don't mock the database in integration tests. NestJS services need `unplugin-swc` in their `vitest.config.ts` so decorator metadata is emitted for DI (see `services/collector`). Gate Docker-dependent integration tests behind `SKIP_INTEGRATION=1`.
- Monorepo: `services/`, `libs/` (shared contracts), `infra/` (Terraform + docker-compose), `docs/`. Each service is **independently deployable**: its own multi-stage `Dockerfile` (build context = repo root so `@cascade/contracts` resolves), added to `infra/docker-compose.yml` under the `apps` profile (`make up` = infra only; `make stack-up` = full stack). See ADR-0010.
- **Config is env-only, validated with Zod at boot — no inline defaults.** Each service has a per-service Zod env schema (`src/config/env.schema.ts`) parsed once at startup into a frozen, typed `APP_CONFIG`; a missing/invalid var fails fast (12-factor). Infra/peer addresses (`KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_*`, the Project/Schema service's `DATABASE_URL`) are **required** and come from config via container service names — never hardcoded, never `?? 'localhost'`. Only a service's own HTTP `PORT` keeps a default. Document every var in `services/<svc>/.env.example`.
- **Every service exposes `GET /health` (liveness) and `GET /ready` (readiness)** via `@nestjs/terminus` — readiness pings its deps (Kafka and/or Cassandra) and returns `503` when one is down. The Ingestion-Processor is a **hybrid app** (`NestFactory.create` + `connectMicroservice` + `startAllMicroservices`): a Kafka consumer plus a small HTTP server for the probes. See ADR-0010.
- **Shared contracts use Zod as the single source of truth.** Define the schema in `@cascade/contracts` and derive the TS type via `z.infer` (never hand-write a parallel `interface` + validator — they drift). The canonical event envelope is `rawEventSchema`/`RawEvent` (see ADR-0004, `docs/contracts/events.md`). It separates `occurredAt` (event time, from the client) from `receivedAt` (ingest time, stamped by the Collector); Cassandra `time_window` buckets by `occurredAt`. The Collector validates before producing and the Ingestion-Processor validates on consume. HTTP-edge validation derives its schema from the contract (`collectEventSchema` = `rawEventSchema.omit(...).strip()`) rather than re-implementing it; invalid events get a structured `400` and never reach Kafka (ADR-0005).

## Local env gotchas

- Cassandra needs a healthcheck + startup wait; don't connect before it's ready.
- Cassandra datacenter is **`datacenter1`** (default SimpleSnitch ignores `CASSANDRA_DC`). Clients must set `localDataCenter`/`CASSANDRA_LOCAL_DC=datacenter1`, or the driver finds no hosts.
- Kafka listener config matters: advertise a host-reachable listener for tooling AND an internal listener for containers. Use the container service name (not `localhost`) for service-to-service; use `127.0.0.1:<mapped-port>` from the host.

## Working agreement (IMPORTANT)

- **Plan first.** For any ticket, produce a short plan (files to touch, approach, tradeoffs) and wait for approval before writing code. No code in the planning step.
- **One Jira ticket at a time.** Reference the ticket key (e.g. KAN-16) in the branch and commit.
- **Document after implementing.** When a ticket is done: update the relevant `docs/` page, add/append an ADR if a real decision was made, and update this CLAUDE.md if a convention changed.
- **Definition of done:** code + passing tests + docs updated + Jira ticket moved to Done.

## Phase gate

Currently in **Phase 0 — walking skeleton** (epic KAN-5). Do not start Phase 1 work until the end-to-end smoke test (KAN-20) passes.

The gate is `npm run smoke` (the `@cascade/e2e` workspace; see README → "End-to-end smoke test"). It stands up real Kafka + Cassandra via Testcontainers, boots all three services in-process, and asserts an event round-trips Collector → Kafka → Cassandra → Query API. **As of KAN-20 this passes** — Phase 1 work may begin. Keep it green; treat a red smoke test as a release blocker. (Note: NestJS's `ServerKafka` postfixes the consumer `groupId` with `-server`, so the Ingestion-Processor's broker-side group is `cascade-ingestion-processor-server`.)
