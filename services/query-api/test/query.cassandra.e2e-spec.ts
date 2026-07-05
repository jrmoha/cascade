import type { Server } from 'node:http';
import { ValidationPipe } from '@nestjs/common';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Client, types } from 'cassandra-driver';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { toHourlyBucket, type RawEvent } from '@cascade/contracts';
import { AppModule } from '../src/app.module';

// Integration test against a real Cassandra (no mocking the DB, per CLAUDE.md).
// Seeds events across two hourly buckets exactly as the Ingestion-Processor
// would, then reads them back through the live GET /query time-range endpoint
// (KAN-25), asserting completeness, ordering, and pagination. Set
// SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('GET /query time-range (integration)', () => {
  let container: StartedTestContainer;
  let seed: Client;
  let app: INestApplication;

  const PROJECT = 'game-1';
  // Five events across two buckets (15:xx and 14:xx), listed newest-first — the
  // order GET /query must return. occurred_at is fixed (not "now"), so the
  // window and bucket enumeration are fully deterministic.
  const EVENTS = [
    { id: '00000000-0000-4000-8000-000000000001', occurredAt: '2026-05-30T15:30:00.000Z' },
    { id: '00000000-0000-4000-8000-000000000002', occurredAt: '2026-05-30T15:20:00.000Z' },
    { id: '00000000-0000-4000-8000-000000000003', occurredAt: '2026-05-30T15:10:00.000Z' },
    { id: '00000000-0000-4000-8000-000000000004', occurredAt: '2026-05-30T14:50:00.000Z' },
    { id: '00000000-0000-4000-8000-000000000005', occurredAt: '2026-05-30T14:40:00.000Z' },
  ];
  const ORDERED_IDS = EVENTS.map((e) => e.id);

  // A window covering both buckets: [14:00, 15:59:59.999] → buckets 15 and 14.
  const FROM = '2026-05-30T14:00:00.000Z';
  const TO = '2026-05-30T15:59:59.999Z';

  beforeAll(async () => {
    container = await new GenericContainer('cassandra:4.1')
      .withExposedPorts(9042)
      .withStartupTimeout(180_000)
      .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
      .start();

    const host = container.getHost();
    const port = container.getMappedPort(9042);

    // Seed schema + rows with a throwaway client (mirrors what the
    // Ingestion-Processor writes; the Query API itself performs no DDL).
    seed = new Client({
      contactPoints: [host],
      protocolOptions: { port },
      localDataCenter: 'datacenter1',
    });
    await seed.connect();
    await seed.execute(
      "CREATE KEYSPACE IF NOT EXISTS cascade WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
    );
    await seed.execute(`
      CREATE TABLE IF NOT EXISTS cascade.raw_events (
        project_id text, time_bucket text, occurred_at timestamp, event_id uuid,
        type text, received_at timestamp, payload text,
        session_id text, actor_id text, source text,
        PRIMARY KEY ((project_id, time_bucket), occurred_at, event_id)
      ) WITH CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)`);

    const insert = `INSERT INTO cascade.raw_events
      (project_id, time_bucket, occurred_at, event_id, type, received_at, payload, session_id, actor_id, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    for (const e of EVENTS) {
      await seed.execute(
        insert,
        [
          PROJECT,
          toHourlyBucket(e.occurredAt),
          new Date(e.occurredAt),
          types.Uuid.fromString(e.id),
          'level_complete',
          new Date(e.occurredAt),
          JSON.stringify({ level: 3 }),
          null,
          null,
          null,
        ],
        { prepare: true },
      );
    }

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
    '/query?' + new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));

  it('returns every event in the window across both buckets, newest-first', async () => {
    const res = await request(server())
      .get(q({ projectId: PROJECT, from: FROM, to: TO }))
      .expect(200);

    expect(res.body.projectId).toBe(PROJECT);
    expect(res.body.from).toBe(FROM);
    expect(res.body.to).toBe(TO);
    expect(res.body.count).toBe(5);
    expect((res.body.events as RawEvent[]).map((e) => e.eventId)).toEqual(ORDERED_IDS);
    // Fits in one page, so no continuation cursor.
    expect(res.body.nextCursor).toBeUndefined();
  });

  it('trims to the requested sub-window (excludes events outside [from, to])', async () => {
    // Only the three 15:xx events.
    const res = await request(server())
      .get(q({ projectId: PROJECT, from: '2026-05-30T15:00:00.000Z', to: TO }))
      .expect(200);

    expect((res.body.events as RawEvent[]).map((e) => e.eventId)).toEqual(ORDERED_IDS.slice(0, 3));
  });

  it('paginates a multi-bucket window completely and in order with no duplicates', async () => {
    const collected: string[] = [];
    let cursor: string | undefined;
    let pages = 0;

    do {
      const res = await request(server())
        .get(q({ projectId: PROJECT, from: FROM, to: TO, limit: 2, ...(cursor && { cursor }) }))
        .expect(200);
      collected.push(...(res.body.events as RawEvent[]).map((e) => e.eventId));
      cursor = res.body.nextCursor;
      expect(++pages).toBeLessThanOrEqual(10); // guard against a paging loop
    } while (cursor);

    // Completeness + ordering + uniqueness across the whole multi-bucket window.
    expect(collected).toEqual(ORDERED_IDS);
    expect(new Set(collected).size).toBe(collected.length);
  });

  it('returns an empty result for an unknown projectId', async () => {
    const res = await request(server())
      .get(q({ projectId: 'does-not-exist', from: FROM, to: TO }))
      .expect(200);
    expect(res.body.count).toBe(0);
    expect(res.body.events).toEqual([]);
    expect(res.body.nextCursor).toBeUndefined();
  });

  it('rejects a request missing projectId / from / to with 400', async () => {
    await request(server()).get('/query').expect(400);
    await request(server())
      .get(q({ projectId: PROJECT }))
      .expect(400);
    await request(server())
      .get(q({ projectId: PROJECT, from: FROM }))
      .expect(400);
  });

  it('rejects a window where from is after to with 400', async () => {
    await request(server())
      .get(q({ projectId: PROJECT, from: TO, to: FROM }))
      .expect(400);
  });

  it('rejects a malformed cursor with 400', async () => {
    await request(server())
      .get(q({ projectId: PROJECT, from: FROM, to: TO, cursor: 'not-a-real-cursor' }))
      .expect(400);
  });
});
