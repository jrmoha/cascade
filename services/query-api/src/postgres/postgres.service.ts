import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { Pool, type QueryResult, type QueryResultRow } from 'pg';
import { APP_CONFIG } from '../config/config.module';
import type { QueryApiConfig } from '../config/env.schema';

/**
 * Owns the `pg` connection pool for the read path. Like {@link CassandraService},
 * this performs **no DDL or migrations** — the Aggregator owns the funnel and
 * retention summary tables (ADR-0015 §2 / ADR-0017); the Query API only connects
 * (with retry, since Postgres can refuse connections on cold start) and reads
 * them. Raw `pg`, not Prisma — Prisma stays fenced to the Project/Schema service
 * (ADR-0011).
 */
@Injectable()
export class PostgresService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(PostgresService.name);
  private readonly pool: Pool;

  constructor(@Inject(APP_CONFIG) config: QueryApiConfig) {
    this.pool = new Pool({ connectionString: config.DATABASE_URL });
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.connectWithRetry();
    this.logger.log('Postgres ready (read-only)');
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
