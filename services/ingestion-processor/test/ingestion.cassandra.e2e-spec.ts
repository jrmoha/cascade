import { ConfigService } from '@nestjs/config';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RawEvent } from '@cascade/contracts';
import { CassandraService } from '../src/cassandra/cassandra.service';
import { RawEventRepository } from '../src/processor/raw-event.repository';

// Integration test against a real Cassandra (no mocking the DB, per CLAUDE.md).
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Ingestion → Cassandra (integration)', () => {
  let container: StartedTestContainer;
  let cassandra: CassandraService;
  let repository: RawEventRepository;

  beforeAll(async () => {
    container = await new GenericContainer('cassandra:4.1')
      .withExposedPorts(9042)
      .withStartupTimeout(180_000)
      .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
      .start();

    const config = new ConfigService({
      CASSANDRA_CONTACT_POINTS: container.getHost(),
      CASSANDRA_PORT: String(container.getMappedPort(9042)),
      CASSANDRA_LOCAL_DC: 'datacenter1',
    });

    cassandra = new CassandraService(config);
    await cassandra.onApplicationBootstrap(); // connect (with retry) + ensure schema
    repository = new RawEventRepository(cassandra);
  });

  afterAll(async () => {
    await cassandra?.onModuleDestroy();
    await container?.stop();
  });

  const event = (overrides: Partial<RawEvent> = {}): RawEvent => ({
    eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
    projectId: 'game-1',
    type: 'level_complete',
    timestamp: '2026-05-30T15:16:50.165Z',
    payload: { level: 3 },
    ...overrides,
  });

  async function rowsFor(projectId: string, timeWindow: string) {
    const rs = await cassandra.execute(
      'SELECT project_id, time_window, event_id, type, event_time, payload FROM cascade.raw_events WHERE project_id = ? AND time_window = ?',
      [projectId, timeWindow],
      { prepare: true },
    );
    return rs.rows;
  }

  it('persists a consumed event as a queryable row', async () => {
    const e = event();
    await repository.insert(e);

    const rows = await rowsFor('game-1', '2026-05-30T15');
    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.event_id.toString()).toBe(e.eventId);
    expect(row.type).toBe('level_complete');
    expect(JSON.parse(row.payload)).toEqual({ level: 3 });
    expect(new Date(row.event_time).toISOString()).toBe(e.timestamp);
  });

  it('is idempotent: re-inserting the same event yields exactly one row', async () => {
    const e = event({ projectId: 'game-dup', eventId: '11111111-1111-4111-8111-111111111111' });
    await repository.insert(e);
    await repository.insert(e);
    await repository.insert(e);

    const rows = await rowsFor('game-dup', '2026-05-30T15');
    expect(rows).toHaveLength(1);
  });
});
