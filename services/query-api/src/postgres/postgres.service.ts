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
 * Owns the `pg` connection pools for the read path. Like {@link CassandraService},
 * this performs **no DDL or migrations** — the Aggregator owns the funnel and
 * retention summary tables (ADR-0015 §2 / ADR-0017); the Query API only connects
 * (with retry, since Postgres can refuse connections on cold start) and reads
 * them. Raw `pg`, not Prisma — Prisma stays fenced to the Project/Schema service
 * (ADR-0011).
 *
 * There are **two** pools (KAN-41, ADR-0019 §2): the {@link query} primary pool
 * and a {@link replicaQuery} pool pointed at the streaming read replica. The
 * eventually-consistent analytics reads (funnel, retention) use the replica; when
 * no `DATABASE_REPLICA_URL` is configured the replica pool is the primary, so
 * single-node dev/test is unaffected.
 */
@Injectable()
export class PostgresService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(PostgresService.name);
  private readonly pool: Pool;
  private readonly replicaPool: Pool;
  /** Whether a distinct replica URL was configured (for logging/clarity). */
  private readonly hasReplica: boolean;

  constructor(@Inject(APP_CONFIG) config: QueryApiConfig) {
    this.pool = new Pool({ connectionString: config.DATABASE_URL });
    this.hasReplica = Boolean(config.DATABASE_REPLICA_URL);
    // Fall back to the primary when no replica is configured — keeps the two-pool
    // API uniform without a separate code path for single-node environments.
    this.replicaPool = this.hasReplica
      ? new Pool({ connectionString: config.DATABASE_REPLICA_URL })
      : this.pool;
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.connectWithRetry(this.pool, 'primary');
    if (this.hasReplica) await this.connectWithRetry(this.replicaPool, 'replica');
    this.logger.log(
      `Postgres ready (read-only) — analytics reads from ${this.hasReplica ? 'replica' : 'primary (no replica configured)'}`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
    if (this.hasReplica) await this.replicaPool.end();
  }

  /** Query the **primary** (read-your-writes / connection-health path). */
  query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<R>> {
    return this.pool.query<R>(text, params);
  }

  /**
   * Query the **read replica** — for eventually-consistent analytics reads that
   * tolerate replication lag (ADR-0019 §2). Falls back to the primary when no
   * replica is configured.
   */
  replicaQuery<R extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<QueryResult<R>> {
    return this.replicaPool.query<R>(text, params);
  }

  private async connectWithRetry(
    pool: Pool,
    label: string,
    attempts = 10,
    delayMs = 3000,
  ): Promise<void> {
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        const client = await pool.connect();
        client.release();
        return;
      } catch (err) {
        if (attempt === attempts) throw err;
        this.logger.warn(
          `Postgres ${label} not ready (attempt ${attempt}/${attempts}): ${(err as Error).message}. Retrying in ${delayMs}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }
}
