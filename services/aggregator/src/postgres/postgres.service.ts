import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { Pool, type QueryResult, type QueryResultRow } from 'pg';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { PgMigrator } from './pg-migrator';

/**
 * Owns the `pg` connection pool for the Aggregator's relational read models
 * (funnel/retention summaries, ADR-0015). Connects (with retry, since Postgres
 * can refuse connections on cold start) and applies the committed
 * `migrations/postgres/*.sql` via {@link PgMigrator} before the consumer starts.
 *
 * Raw `pg`, not Prisma: Prisma stays fenced to the Project/Schema service
 * (ADR-0011). The Aggregator owns its own tables in the shared database. Phase 1
 * skeleton (KAN-31): no summary tables yet.
 */
@Injectable()
export class PostgresService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(PostgresService.name);
  private readonly pool: Pool;

  constructor(@Inject(APP_CONFIG) config: AggregatorConfig) {
    this.pool = new Pool({ connectionString: config.DATABASE_URL });
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.connectWithRetry();
    await new PgMigrator(this.pool).run();
    this.logger.log('Postgres ready (migrations applied, pool connected)');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }

  query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<R>> {
    return this.pool.query<R>(text, params);
  }

  private async connectWithRetry(attempts = 10, delayMs = 3000): Promise<void> {
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        const client = await this.pool.connect();
        client.release();
        return;
      } catch (err) {
        if (attempt === attempts) throw err;
        this.logger.warn(
          `Postgres not ready (attempt ${attempt}/${attempts}): ${(err as Error).message}. Retrying in ${delayMs}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }
}
