import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Client, types } from 'cassandra-driver';

export const KEYSPACE = 'cascade';

const CREATE_KEYSPACE = `
  CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`;

const CREATE_RAW_EVENTS = `
  CREATE TABLE IF NOT EXISTS ${KEYSPACE}.raw_events (
    project_id   text,
    time_window  text,
    event_id     uuid,
    type         text,
    event_time   timestamp,
    payload      text,
    PRIMARY KEY ((project_id, time_window), event_id)
  )`;

/**
 * Owns the cassandra-driver client. Connects (with retry, since Cassandra is
 * slow to accept connections on cold start) and ensures the keyspace + table
 * exist before the consumer starts handling messages.
 */
@Injectable()
export class CassandraService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(CassandraService.name);
  private readonly client: Client;

  constructor(config: ConfigService) {
    const contactPoints = (config.get<string>('CASSANDRA_CONTACT_POINTS') ?? 'localhost')
      .split(',')
      .map((p) => p.trim())
      .filter(Boolean);
    const port = Number(config.get<string>('CASSANDRA_PORT') ?? 9042);
    const localDataCenter = config.get<string>('CASSANDRA_LOCAL_DC') ?? 'datacenter1';

    this.client = new Client({ contactPoints, protocolOptions: { port }, localDataCenter });
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.connectWithRetry();
    await this.client.execute(CREATE_KEYSPACE);
    await this.client.execute(CREATE_RAW_EVENTS);
    this.logger.log(`Cassandra ready (keyspace "${KEYSPACE}", table raw_events)`);
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
