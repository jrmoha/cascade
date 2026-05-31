import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { Client } from 'cassandra-driver';
import { Migrator } from './migrator';

/**
 * Standalone migration entrypoint: `npm run migrate -w @cascade/ingestion-processor`.
 * Applies the committed `migrations/*.cql` against the configured Cassandra.
 * Idempotent — safe to run repeatedly (already-applied migrations are skipped).
 */
async function main(): Promise<void> {
  const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? 'localhost')
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  const port = Number(process.env.CASSANDRA_PORT ?? 9042);
  const localDataCenter = process.env.CASSANDRA_LOCAL_DC ?? 'datacenter1';

  const client = new Client({ contactPoints, protocolOptions: { port }, localDataCenter });
  try {
    await client.connect();
    await new Migrator(client).run();
  } finally {
    await client.shutdown();
  }
}

main().catch((err) => {
  Logger.error((err as Error).message, (err as Error).stack, 'migrate');
  process.exitCode = 1;
});
