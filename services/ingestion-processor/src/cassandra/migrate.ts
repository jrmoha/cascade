import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { Client } from 'cassandra-driver';
import { cassandraEnvSchema } from '../config/env.schema';
import { Migrator } from './migrator';

/**
 * Standalone migration entrypoint: `npm run migrate -w @cascade/ingestion-processor`.
 * Applies the committed `migrations/*.cql` against the configured Cassandra.
 * Idempotent — safe to run repeatedly (already-applied migrations are skipped).
 * Cassandra connection vars are required and Zod-validated (no defaults), the
 * same contract the service uses on boot.
 */
async function main(): Promise<void> {
  const config = cassandraEnvSchema.parse(process.env);

  const client = new Client({
    contactPoints: config.CASSANDRA_CONTACT_POINTS,
    protocolOptions: { port: config.CASSANDRA_PORT },
    localDataCenter: config.CASSANDRA_LOCAL_DC,
  });
  try {
    await client.connect();
    await new Migrator(client, {
      localDc: config.CASSANDRA_LOCAL_DC,
      replicationFactor: config.CASSANDRA_REPLICATION_FACTOR,
    }).run();
  } finally {
    await client.shutdown();
  }
}

main().catch((err) => {
  Logger.error((err as Error).message, (err as Error).stack, 'migrate');
  process.exitCode = 1;
});
