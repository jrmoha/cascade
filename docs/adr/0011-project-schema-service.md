# 0011 — Project/Schema service: Postgres via Prisma, hashed API keys, JSON-Schema event schemas

**Status:** Accepted (§1 amended by [ADR-0014](0014-nestjs-11-prisma-7-upgrade.md): under Prisma 7
the runtime client connects through the `@prisma/adapter-pg` driver adapter and the Migrate
datasource URL lives in `prisma.config.ts`, not `schema.prisma` — Prisma remains fenced to
persistence.)

## Context

[ADR-0009](0009-service-boundaries-and-communication.md) reserved a **Project/Schema** service
that "owns project metadata, schemas, and API keys in PostgreSQL" and named the one justified
synchronous dependency on it (`Collector → Project/Schema`, for key/schema validation at ingest).
KAN-28 builds that service. Three concrete decisions had to be pinned down before writing it:
how the relational schema is defined and migrated, how API keys are stored and verified, and how
per-project event schemas are represented. This is the bounded context that "earns" Postgres —
relational, low-volume, consistency-sensitive config — so it is also where we deliberately diverge
from the raw-driver style used elsewhere.

## Decision

### 1. Persistence: PostgreSQL, schema owned by **Prisma**, applied via versioned migrations

The service uses Prisma (`@prisma/client`) over Postgres. `prisma/schema.prisma` is the single
source of truth for the **DB**; `prisma migrate dev` generates versioned SQL migrations under
`prisma/migrations/`, which are applied with `prisma migrate deploy` — **never** `db push` /
`synchronize` in any environment. Migrations are applied **on service bootstrap**
(`DatabaseService.onApplicationBootstrap`) and via `npm run migrate`, mirroring the
Ingestion-Processor's migrate-on-boot contract ([ADR-0007](0007-cassandra-raw-events-model.md)).
`prisma` is kept as a runtime dependency so the migrate CLI is available in the container.

This is a **deliberate exception** to the repo's otherwise raw-driver approach (cassandra-driver
and `pg`/kafkajs are used raw): Prisma owns only the relational **DB schema**. The **wire/API
contracts** remain Zod schemas in `@cascade/contracts`
([ADR-0004](0004-canonical-event-contract.md)'s convention) — the two layers are kept separate and
mapped explicitly. Prisma is not allowed to leak into the cross-service contract surface.

### 2. API keys: random `cas_<prefix>.<secret>`, **argon2-hashed**, looked up by prefix

Keys are minted as `cas_<id8>.<secret32>` (CSPRNG). Only an **argon2** hash of the secret half is
stored (`api_keys.hash`); the plaintext is returned exactly once at issue and never again. The
non-secret `cas_<id8>` **prefix** is stored in a **unique-indexed** column so verification is one
indexed lookup followed by a single `argon2.verify` — no full-table scan, cheap enough for the
Collector's hot path. Keys are revocable (`revoked_at`); a revoked key fails verification. Verify
returns `{ valid: false }` for malformed/unknown/revoked/mismatched keys alike and never throws —
an invalid key is data, not an error, and the response does not reveal which check failed.

### 3. Event schemas: **JSON Schema in a `jsonb` column**, unique per `(projectId, eventType)`

Each registered schema is an arbitrary JSON Schema document stored in `event_schemas.json_schema`
(`jsonb`), unique on `(project_id, event_type)` so it is retrievable by that pair and re-registering
upserts in place. Storing the schema as data (not code) is what lets the Collector fetch it and
validate a project's events **dynamically** in KAN-30 — a project can add an event type with no
service redeploy.

## Alternatives considered

- **Hand-rolled SQL migrator (mirror the Cassandra `Migrator`).** Maximally consistent with the
  repo and zero new heavy deps. Rejected for the relational store because a recognised migration
  tool (versioned up-migrations, drift detection, a managed `_prisma_migrations` ledger) is part of
  the "reproducible schema" story this context is meant to demonstrate, and Postgres — unlike
  Cassandra — has first-class tooling. `node-pg-migrate` was the lighter middle ground; Prisma was
  chosen for its typed client and migration ergonomics.
- **`bcrypt` instead of `argon2`.** Both are sound password hashes with prefix-lookup. argon2 is
  the current OWASP first choice (memory-hard) and was selected; the prefix-lookup design is
  identical either way.
- **A dedicated `event_schema_versions` table / per-row schema validation on register.** Deferred.
  Upsert-in-place keeps KAN-28 focused; schema **content** validation and the Collector-side
  dynamic validation are KAN-30.

## Consequences

**Positive:**

- The service slots into the ADR-0009 topology: owns Postgres, touches no Kafka/Cassandra/Redis,
  and exposes the verify-key/fetch-schema lookups the Collector needs in KAN-30.
- Reproducible, versioned schema; migrate-on-boot keeps the convention uniform across services.
- API keys are stored like passwords (argon2), revocable, and verifiable in one indexed lookup.
- Event schemas as jsonb data enable dynamic, redeploy-free validation downstream.

**Trade-offs:**

- Prisma is a heavier dependency and the one ORM in an otherwise raw-driver codebase; it is fenced
  to the persistence layer to contain that divergence, and `prisma` ships in the runtime image so
  migrate-on-boot works.
- Migrate-on-boot means concurrent replicas race to apply migrations; Prisma's advisory lock makes
  this safe but serialises a cold start.
- argon2 is a native module (prebuilt binaries for `node:22-slim`); it must build/resolve in the
  Docker image and in CI.
