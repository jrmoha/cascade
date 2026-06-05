# 0014 — Upgrade to NestJS 11 and Prisma 7 (driver-adapter Postgres access)

**Status:** Accepted

## Context

A dependency-audit run (`npm audit fix --force`) force-bumped major versions across the monorepo
and left it in a **broken, incoherent** state: `@prisma/client@7` paired with the `prisma@6` CLI,
and `@nestjs/common@10` paired with `@nestjs/core@11` (Nest requires those to share a major). The
Project/Schema service then failed to boot with `Cannot find module '@prisma/client/runtime/library.js'`
— Prisma 7 **removes the Rust query-engine binary** (and that `library.js` entrypoint), running
queries through a WASM **query compiler** plus a JavaScript **driver adapter** instead.

Rather than revert, we chose to **migrate forward** to a coherent target set and absorb the
breaking changes deliberately (the audit's choices were not, by themselves, a decision).

## Decision

### Target versions (coherent)

- **NestJS 11** for all `@nestjs/*` runtime + testing packages (`@nestjs/config` v4 is the
  Nest-11-compatible line; `@nestjs/terminus` v11).
- **Prisma 7** for both `@prisma/client` and the `prisma` CLI, **plus** the `@prisma/adapter-pg`
  driver adapter and its `pg` peer.
- **Testcontainers 12** (`testcontainers`, `@testcontainers/kafka`, `@testcontainers/postgresql`).

All five `package.json`s and the `e2e` workspace were normalised to this set so no two packages
straddle a major boundary.

### Prisma 7: Postgres now goes through the `pg` driver adapter

Prisma 7 has no embedded query engine. Consequences, all confined to the Project/Schema service:

1. **Runtime client.** `DatabaseService extends PrismaClient` now passes a `PrismaPg`
   adapter to `super({ adapter: new PrismaPg(config.DATABASE_URL) })`, built from the same
   validated `DATABASE_URL`. This is still fenced to persistence — Prisma does not leak into the
   cross-service surface (ADR-0011 holds).
2. **No `datasource.url` in the schema.** Prisma 7 forbids it. The Migrate connection URL moved to
   a new **`prisma.config.ts`** at the service root, which also declares the schema/migrations
   paths. The datasource block in `schema.prisma` keeps only `provider = "postgresql"`.
3. **`binaryTargets` removed.** With the WASM query compiler there is no per-platform engine to
   embed, so the generated client is portable across local dev (macOS) and the `node:22-slim`
   image alike — the previous `["native", "debian-openssl-3.0.x"]` pin is obsolete.
4. **No `.env` auto-loading.** Prisma 7's CLI no longer reads `.env`. `prisma.config.ts`
   best-effort loads the service-local then repo-root `.env` via Node's built-in
   `process.loadEnvFile` (already-set vars win, so container-injected env always takes precedence),
   and **includes the datasource only when `DATABASE_URL` is set** so build-time `prisma generate`
   (no database) still works.
5. **Migrate-on-boot** invokes `prisma migrate deploy --config <abs>/prisma.config.ts` (explicit
   config path, not cwd auto-discovery) so it is correct regardless of launch directory. `prisma`
   stays a **runtime** dependency and `prisma.config.ts` is copied into the runtime image.

### NestJS 11

No application-code changes were required. The custom `@nestjs/terminus` health indicators
(`PrismaHealthIndicator`, `RedisHealthIndicator`, the two `CassandraHealthIndicator`s) use the
`HealthIndicator`/`getStatus`/`HealthCheckError` API, which is **deprecated but still present** in
terminus 11.1 — they compile and run unchanged. Migrating them to the newer
`HealthIndicatorService` is deferred as non-urgent cleanup. No wildcard routes exist, so Express 5
(under platform-express 11) needed no route-syntax changes.

## Consequences

- The Project/Schema service boots again; the **Phase-0 smoke gate stays green** (Collector →
  Kafka → Cassandra → Query API, with Postgres + Redis), validating the upgrade end-to-end.
- DB access for the config store is now adapter-based — a small architectural shift worth noting
  for anyone touching `DatabaseService` (amends ADR-0011 §1).
- The terminus indicators carry a deprecation that a future ticket should clear before terminus
  removes the old API.
- Outstanding `npm audit` advisories were **not** chased here; security bumps should land through
  their own scoped change, not a force-upgrade sweep.
