import type { Server } from 'node:http';
import { ValidationPipe } from '@nestjs/common';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Client } from 'cassandra-driver';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { toMinuteBucket, type CountBucket } from '@cascade/contracts';
import { AppModule } from '../src/app.module';

// Integration test against a real Cassandra (no mocking the DB, per CLAUDE.md).
// Seeds the Aggregator's event-count `counter` tables exactly as the Aggregator
// would (UPDATE count = count + N), then reads them back through the live
// GET /counts endpoint (KAN-36), asserting the CQRS read boundary: analytics is
// served from the derived counter tables, never raw_events. Set
// SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('GET /counts (integration)', () => {
  let container: StartedTestContainer;
  let seed: Client;
  let app: INestApplication;

  const PROJECT = 'game-1';
  // Two hours of activity. login happens in both hours; score only in the first.
  const HOUR_A = '2026-05-30T10'; // occurredAt 10:xx
  const HOUR_B = '2026-05-30T11'; // occurredAt 11:xx
  const T_A = '2026-05-30T10:15:00.000Z';
  const T_B = '2026-05-30T11:20:00.000Z';

  // [from, to] covering both hours.
  const FROM = '2026-05-30T10:00:00.000Z';
  const TO = '2026-05-30T11:59:59.999Z';

  beforeAll(async () => {
    container = await new GenericContainer('cassandra:4.1')
      .withExposedPorts(9042)
      .withStartupTimeout(180_000)
      .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
      .start();

    const host = container.getHost();
    const port = container.getMappedPort(9042);

    // Seed schema + counters with a throwaway client (mirrors what the
    // Aggregator writes; the Query API itself performs no DDL).
    seed = new Client({
      contactPoints: [host],
      protocolOptions: { port },
      localDataCenter: 'datacenter1',
    });
    await seed.connect();
    await seed.execute(
      "CREATE KEYSPACE IF NOT EXISTS cascade WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
    );
    for (const table of ['event_counts_by_minute', 'event_counts_by_hour']) {
      await seed.execute(`
        CREATE TABLE IF NOT EXISTS cascade.${table} (
          project_id text, time_bucket text, event_type text, count counter,
          PRIMARY KEY ((project_id, time_bucket), event_type)
        )`);
    }

    // Apply the same counter increments the Aggregator would, at both granularities.
    const bump = async (table: string, bucket: string, type: string, by: number): Promise<void> => {
      await seed.execute(
        `UPDATE cascade.${table} SET count = count + ? WHERE project_id = ? AND time_bucket = ? AND event_type = ?`,
        [by, PROJECT, bucket, type],
        { prepare: true },
      );
    };
    // Hour A: 5 login + 2 score · Hour B: 3 login.
    await bump('event_counts_by_hour', HOUR_A, 'login', 5);
    await bump('event_counts_by_hour', HOUR_A, 'score', 2);
    await bump('event_counts_by_hour', HOUR_B, 'login', 3);
    await bump('event_counts_by_minute', toMinuteBucket(T_A), 'login', 5);
    await bump('event_counts_by_minute', toMinuteBucket(T_A), 'score', 2);
    await bump('event_counts_by_minute', toMinuteBucket(T_B), 'login', 3);

    // Point the app at the container and boot it (triggers Cassandra connect).
    process.env.CASSANDRA_CONTACT_POINTS = host;
    process.env.CASSANDRA_PORT = String(port);
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
    process.env.CASSANDRA_REPLICATION_FACTOR = '1';
    process.env.CASSANDRA_CONSISTENCY = 'local_quorum';
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    );
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    await seed?.shutdown();
    await container?.stop();
  });

  const server = (): Server => app.getHttpServer();
  const q = (params: Record<string, string | number>): string =>
    '/counts?' + new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));
  // Compare ignoring order (bucket walk order is deterministic, but assert on content).
  const sortBuckets = (b: CountBucket[]): CountBucket[] =>
    [...b].sort(
      (x, y) => x.bucket.localeCompare(y.bucket) || x.eventType.localeCompare(y.eventType),
    );

  it('serves hourly counts per (bucket, type) across the window, defaulting to hour', async () => {
    const res = await request(server())
      .get(q({ projectId: PROJECT, from: FROM, to: TO }))
      .expect(200);

    expect(res.body.projectId).toBe(PROJECT);
    expect(res.body.granularity).toBe('hour');
    expect(sortBuckets(res.body.buckets)).toEqual([
      { bucket: HOUR_A, eventType: 'login', count: 5 },
      { bucket: HOUR_A, eventType: 'score', count: 2 },
      { bucket: HOUR_B, eventType: 'login', count: 3 },
    ]);
  });

  it('serves minute counts when granularity=minute', async () => {
    const res = await request(server())
      .get(q({ projectId: PROJECT, from: FROM, to: TO, granularity: 'minute' }))
      .expect(200);

    expect(res.body.granularity).toBe('minute');
    expect(sortBuckets(res.body.buckets)).toEqual([
      { bucket: toMinuteBucket(T_A), eventType: 'login', count: 5 },
      { bucket: toMinuteBucket(T_A), eventType: 'score', count: 2 },
      { bucket: toMinuteBucket(T_B), eventType: 'login', count: 3 },
    ]);
  });

  it('narrows to a single event type with ?type=', async () => {
    const res = await request(server())
      .get(q({ projectId: PROJECT, from: FROM, to: TO, type: 'score' }))
      .expect(200);

    expect(res.body.buckets).toEqual([{ bucket: HOUR_A, eventType: 'score', count: 2 }]);
  });

  it('returns an empty series for an unknown projectId', async () => {
    const res = await request(server())
      .get(q({ projectId: 'nope', from: FROM, to: TO }))
      .expect(200);
    expect(res.body.buckets).toEqual([]);
  });

  it('rejects from > to and a missing projectId with 400', async () => {
    await request(server())
      .get(q({ projectId: PROJECT, from: TO, to: FROM }))
      .expect(400);
    await request(server())
      .get(q({ from: FROM, to: TO }))
      .expect(400);
  });

  it('rejects a minute window wider than the cap with 400', async () => {
    // > 1440 minutes (24h) at minute granularity.
    await request(server())
      .get(
        q({
          projectId: PROJECT,
          from: '2026-05-30T00:00:00.000Z',
          to: '2026-06-01T00:00:00.000Z',
          granularity: 'minute',
        }),
      )
      .expect(400);
  });
});
