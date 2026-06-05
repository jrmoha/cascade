import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';
import { APP_CONFIG } from '../config/config.module';
import type { ProjectSchemaConfig } from '../config/env.schema';

/**
 * Absolute path to the Prisma 7 config file. It carries the Migrate datasource
 * URL and schema/migrations paths; passing it explicitly (rather than relying on
 * cwd auto-discovery) keeps `migrate deploy` correct regardless of where the
 * process is launched from.
 */
const CONFIG_PATH = resolve(__dirname, '..', '..', 'prisma.config.ts');

/**
 * Owns the Prisma client (the single Postgres connection pool). On bootstrap it
 * applies the committed migrations with `prisma migrate deploy` — the same
 * migrate-on-boot contract the Ingestion-Processor uses for Cassandra — so the
 * schema is present and up to date before any request is served, then connects.
 * The versioned `prisma/migrations/*` are the single source of truth for the
 * schema; this service applies them, it does not define DDL inline.
 */
@Injectable()
export class DatabaseService
  extends PrismaClient
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private readonly logger = new Logger(DatabaseService.name);

  constructor(@Inject(APP_CONFIG) config: ProjectSchemaConfig) {
    // Prisma 7 has no built-in query engine: the runtime client talks to
    // Postgres through the `pg` driver adapter. The connection string is the
    // same validated `DATABASE_URL` Migrate uses (via prisma.config.ts).
    super({ adapter: new PrismaPg(config.DATABASE_URL) });
  }

  async onApplicationBootstrap(): Promise<void> {
    this.deployMigrations(); // synchronous (execFileSync) — blocks until migrations are applied
    await this.connectWithRetry();
    this.logger.log('Postgres ready (migrations applied, client connected)');
  }

  async onModuleDestroy(): Promise<void> {
    await this.$disconnect();
  }

  /**
   * Runs `prisma migrate deploy` against the configured `DATABASE_URL`.
   * Idempotent — already-applied migrations are skipped. A failure throws and
   * aborts boot (we must not serve traffic against a stale schema). Synchronous
   * on purpose: the call site relies on it blocking before `$connect`.
   */
  private deployMigrations(): void {
    this.logger.log('Applying database migrations…');
    execFileSync('npx', ['--no-install', 'prisma', 'migrate', 'deploy', '--config', CONFIG_PATH], {
      stdio: 'inherit',
      env: process.env,
    });
  }

  /**
   * Connects with bounded retry — Postgres can still be refusing connections on
   * a cold start in environments without a `depends_on: service_healthy` gate
   * (e.g. k8s). Mirrors the Ingestion-Processor's `CassandraService`.
   */
  private async connectWithRetry(attempts = 10, delayMs = 3000): Promise<void> {
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        await this.$connect();
        return;
      } catch (err) {
        if (attempt === attempts) throw err;
        this.logger.warn(
          `Postgres not ready (attempt ${attempt}/${attempts}): ${(err as Error).message}. Retrying in ${delayMs}ms`,
        );
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }
  }
}
