import { Client } from 'cassandra-driver';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { KEYSPACE, Migrator } from '../src/cassandra/migrator';

// Integration test for the schema migration runner (KAN-24): applies the
// committed migrations against a real Cassandra and is idempotent on re-run.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Migrator (integration)', () => {
  let container: StartedTestContainer;
  let client: Client;

  beforeAll(async () => {
    container = await new GenericContainer('cassandra:4.1')
      .withExposedPorts(9042)
      .withStartupTimeout(180_000)
      .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
      .start();

    client = new Client({
      contactPoints: [container.getHost()],
      protocolOptions: { port: container.getMappedPort(9042) },
      localDataCenter: 'datacenter1',
    });
    await client.connect();
  });

  afterAll(async () => {
    await client?.shutdown();
    await container?.stop();
  });

  async function appliedIds(): Promise<string[]> {
    const rs = await client.execute(`SELECT id FROM ${KEYSPACE}.schema_migrations`);
    return rs.rows.map((r) => r.get('id') as string).sort();
  }

  it('creates the keyspace, tracking table, and raw_events with the expected key', async () => {
    await new Migrator(client).run();

    expect(await appliedIds()).toContain('0001_create_raw_events');

    // The query-first key: partition (project_id, time_bucket), clustering
    // (occurred_at, event_id) — verified against system_schema.
    const cols = await client.execute(
      `SELECT column_name, kind, position FROM system_schema.columns
       WHERE keyspace_name = ? AND table_name = 'raw_events'`,
      [KEYSPACE],
      { prepare: true },
    );
    const byKind = (kind: string) =>
      cols.rows
        .filter((r) => r.get('kind') === kind)
        .sort((a, b) => Number(a.get('position')) - Number(b.get('position')))
        .map((r) => r.get('column_name'));

    expect(byKind('partition_key')).toEqual(['project_id', 'time_bucket']);
    expect(byKind('clustering')).toEqual(['occurred_at', 'event_id']);
  });

  it('is idempotent: a second run applies nothing new', async () => {
    const before = await appliedIds();
    await new Migrator(client).run();
    const after = await appliedIds();
    expect(after).toEqual(before);
  });

  it('sets a 30-day default TTL on raw_events', async () => {
    const rs = await client.execute(
      `SELECT default_time_to_live FROM system_schema.tables
       WHERE keyspace_name = ? AND table_name = 'raw_events'`,
      [KEYSPACE],
      { prepare: true },
    );
    expect(rs.rows[0].get('default_time_to_live')).toBe(2592000);
  });
});
