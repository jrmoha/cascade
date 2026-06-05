import path from 'node:path';
import { defineConfig } from 'prisma/config';

/**
 * Prisma 7 configuration (KAN-30 follow-up: NestJS 11 + Prisma 7 upgrade).
 *
 * Prisma 7 removed `datasource.url` from `schema.prisma` and no longer
 * auto-loads `.env`. The connection URL for **Migrate** (CLI: `migrate deploy`,
 * applied on boot by {@link DatabaseService} and via `npm run migrate`) now
 * lives here; the **runtime** client gets its connection from the
 * `@prisma/adapter-pg` driver adapter wired up in `DatabaseService`. See
 * ADR-0011.
 *
 * The datasource is included **only when `DATABASE_URL` is set**: build-time
 * `prisma generate` (e.g. the Docker build stage) runs without a database and
 * must not require it, whereas migrate/introspection do.
 */

// Prisma 7's CLI no longer reads .env automatically. Best-effort load the
// service-local then repo-root .env (mirroring the service's ConfigModule
// order) using Node's built-in loader, so a bare `npm run migrate` from a shell
// still finds DATABASE_URL. In containers the var is injected directly, so the
// missing files here are expected and ignored. Already-set vars are not
// overridden, so the container/runtime environment always wins.
for (const envPath of [path.join(__dirname, '.env'), path.join(__dirname, '..', '..', '.env')]) {
  try {
    process.loadEnvFile(envPath);
  } catch {
    // No .env at this location — fine.
  }
}

const databaseUrl = process.env.DATABASE_URL;

export default defineConfig({
  schema: path.join(__dirname, 'prisma', 'schema.prisma'),
  migrations: {
    path: path.join(__dirname, 'prisma', 'migrations'),
  },
  ...(databaseUrl ? { datasource: { url: databaseUrl } } : {}),
});
