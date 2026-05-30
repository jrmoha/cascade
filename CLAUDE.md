# CLAUDE.md — Cascade

Real-time event analytics platform. This file is the brief; the full reasoning lives in `docs/00-charter.md`, `docs/blueprint.md`, and `docs/adr/`. Read those before non-trivial work.

## What this is

A self-hostable engine that ingests high-volume events, stores them raw, computes real-time aggregations (counters, funnels, retention, leaderboards), and serves them. Demo domain: game telemetry. Backend + deployment + scaling only — no frontend.

## Architecture (see docs/adr/0001)

Event-driven. Collector → **Kafka** → two independent consumers: Ingestion-Processor (writes raw to **Cassandra**) and Aggregator (derives read models). Query API serves **only** from read models. **PostgreSQL** holds metadata; **Redis** holds hot counters/leaderboards/rate-limits. Services: Collector, Ingestion-Processor, Aggregator, Query API, Project/Schema.

## Non-negotiable rules

- **Write path and read path are separate.** The Query API NEVER scans raw Cassandra live — it reads pre-aggregated views. If a feature seems to need a live raw scan, that's a design smell; stop and flag it.
- **Cassandra is modeled query-first.** Never add a table without stating the exact query it serves and its partition key. Watch partition size; bucket by `project_id + time_window`.
- **All Kafka consumers must be idempotent.** Delivery is at-least-once. Re-processing the same message must not double-count.
- **No new cross-service synchronous call without justification.** Prefer events. Sync calls (gRPC/REST) only for genuine request/response needs.

## Stack & conventions

- NestJS + TypeScript. Node 20+. (Pin versions in package.json.)
- Lint/format: ESLint + Prettier. Commits: Conventional Commits (Commitlint + Husky).
- Tests: Vitest (unit), Testcontainers (integration against real Cassandra/Kafka/Postgres). Don't mock the database in integration tests. NestJS services need `unplugin-swc` in their `vitest.config.ts` so decorator metadata is emitted for DI (see `services/collector`). Gate Docker-dependent integration tests behind `SKIP_INTEGRATION=1`.
- Monorepo: `services/`, `libs/` (shared contracts), `infra/` (Terraform + docker-compose), `docs/`.

## Local env gotchas

- Cassandra needs a healthcheck + startup wait; don't connect before it's ready.
- Kafka listener config matters: advertise a host-reachable listener for tooling AND an internal listener for containers. Use the container service name (not `localhost`) for service-to-service; use `127.0.0.1:<mapped-port>` from the host.

## Working agreement (IMPORTANT)

- **Plan first.** For any ticket, produce a short plan (files to touch, approach, tradeoffs) and wait for approval before writing code. No code in the planning step.
- **One Jira ticket at a time.** Reference the ticket key (e.g. KAN-16) in the branch and commit.
- **Document after implementing.** When a ticket is done: update the relevant `docs/` page, add/append an ADR if a real decision was made, and update this CLAUDE.md if a convention changed.
- **Definition of done:** code + passing tests + docs updated + Jira ticket moved to Done.

## Phase gate

Currently in **Phase 0 — walking skeleton** (epic KAN-5). Do not start Phase 1 work until the end-to-end smoke test (KAN-20) passes.
