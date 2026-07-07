import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { Client, types } from 'cassandra-driver';
import { toDriverConsistency } from '@cascade/contracts';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { KEYSPACE, Migrator } from './migrator';

export { KEYSPACE } from './migrator';

/**
 * Owns the cassandra-driver client for the Aggregator's time-series counter read
 * models (ADR-0015). Connects (with retry, since Cassandra is slow to accept
 * connections on cold start) and runs schema migrations ({@link Migrator}) so
 * the keyspace and tables exist before the consumer starts. The committed
 * `migrations/cassandra/*.cql` files are the single source of truth; this
 * service applies them, it does not define DDL inline. Phase 1 skeleton: no
 * counter tables yet.
 */
@Injectable()
export class CassandraService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(CassandraService.name);
  private readonly client: Client;

  private readonly replication: { localDc: string; replicationFactor: number };

  constructor(@Inject(APP_CONFIG) config: AggregatorConfig) {
    this.client = new Client({
      contactPoints: config.CASSANDRA_CONTACT_POINTS,
      protocolOptions: { port: config.CASSANDRA_PORT },
      localDataCenter: config.CASSANDRA_LOCAL_DC,
      // Explicit write/read consistency (KAN-38, ADR-0019) — never the driver
      // default LOCAL_ONE. Applies to every execute() unless overridden.
      queryOptions: { consistency: toDriverConsistency(config.CASSANDRA_CONSISTENCY) },
    });
    this.replication = {
      localDc: config.CASSANDRA_LOCAL_DC,
      replicationFactor: config.CASSANDRA_REPLICATION_FACTOR,
    };
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.connectWithRetry();
    await new Migrator(this.client, this.replication).run();
    this.logger.log(`Cassandra ready (keyspace "${KEYSPACE}")`);
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.shutdown();
  }

  execute(
    query: string,
    params?: unknown[],
    options?: { prepare?: boolean },
  ): Promise<types.ResultSet> {
    return this.client.execute(query, params, options);
  }

  private async connectWithRetry(attempts = 10, delayMs = 3000): Promise<void> {
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        await this.client.connect();
        return;
      } catch (err) {
        if (attempt === attempts) throw err;
        this.logger.warn(
          `Cassandra not ready (attempt ${attempt}/${attempts}): ${(err as Error).message}. Retrying in ${delayMs}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }
}
