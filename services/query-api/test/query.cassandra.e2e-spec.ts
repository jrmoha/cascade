import type { Server } from 'node:http';
import { ValidationPipe } from '@nestjs/common';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Client, types } from 'cassandra-driver';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { toHourlyBucket } from '@cascade/contracts';
import { AppModule } from '../src/app.module';

// Integration test against a real Cassandra (no mocking the DB, per CLAUDE.md).
// Seeds rows exactly as the Ingestion-Processor would, then reads them back
// through the live GET /query endpoint. Set SKIP_INTEGRATION=1 to skip where
// Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('GET /query (integration)', () => {
  let container: StartedTestContainer;
  let seed: Client;
  let app: INestApplication;

  // Pin "now" so seeded rows and the endpoint's bucket enumeration agree.
  const now = new Date().toISOString();
  const bucket = toHourlyBucket(now);

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
      "CREATE KEYSPACE IF NOT EXISTS cascade WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
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
    await seed.execute(
      insert,
      [
        'game-1',
        bucket,
        new Date(now),
        types.Uuid.fromString('11111111-1111-4111-8111-111111111111'),
        'level_complete',
        new Date(now),
        JSON.stringify({ level: 3 }),
        'sess-9',
        'player-42',
        'unity-sdk@1.4.0',
      ],
      { prepare: true },
    );

    // Point the app at the container and boot it (triggers Cassandra connect).
    process.env.CASSANDRA_CONTACT_POINTS = host;
    process.env.CASSANDRA_PORT = String(port);
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';

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

  it('reads back the stored event for a projectId', async () => {
    // hours=2 spans the current + previous bucket, robust across hour rollover.
    const res = await request(server()).get('/query?projectId=game-1&hours=2').expect(200);

    expect(res.body.projectId).toBe('game-1');
    expect(res.body.count).toBe(1);
    expect(res.body.events).toEqual([
      {
        eventId: '11111111-1111-4111-8111-111111111111',
        projectId: 'game-1',
        type: 'level_complete',
        occurredAt: now,
        receivedAt: now,
        payload: { level: 3 },
        sessionId: 'sess-9',
        actorId: 'player-42',
        source: 'unity-sdk@1.4.0',
      },
    ]);
  });

  it('returns an empty result for an unknown projectId', async () => {
    const res = await request(server()).get('/query?projectId=does-not-exist').expect(200);
    expect(res.body.count).toBe(0);
    expect(res.body.events).toEqual([]);
  });

  it('rejects a request missing projectId with 400', async () => {
    await request(server()).get('/query').expect(400);
  });
});
