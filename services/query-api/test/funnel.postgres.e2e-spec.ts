import type { Server } from 'node:http';
import { Module, ValidationPipe, type INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppConfigModule } from '../src/config/config.module';
import { PostgresModule } from '../src/postgres/postgres.module';
import { FunnelModule } from '../src/funnel/funnel.module';

// Integration test for the Query API funnel endpoint against a real Postgres
// (Testcontainers, no mocks per CLAUDE.md). Seeds the Aggregator-owned
// `funnel_actor_steps` table with several actor journeys (one full, one partial,
// one drop-off, and one OUT-OF-ORDER), then asserts the ordered-conversion math.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

@Module({ imports: [AppConfigModule, PostgresModule, FunnelModule] })
class FunnelApiTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Query API funnel (integration)', () => {
  const PROJECT = 'rpg';
  const FROM = '2026-05-01T00:00:00.000Z';
  const TO = '2026-05-31T23:59:59.999Z';

  let pgContainer: StartedPostgreSqlContainer;
  let app: INestApplication;
  let seed: Pool;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();

    // Cassandra + Redis vars are required by the env schema but unused here.
    process.env.CASSANDRA_CONTACT_POINTS = 'localhost';
    process.env.CASSANDRA_PORT = '9042';
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
    process.env.CASSANDRA_REPLICATION_FACTOR = '1';
    process.env.CASSANDRA_CONSISTENCY = 'local_quorum';
    process.env.REDIS_HOST = 'localhost';
    process.env.REDIS_PORT = '6379';
    process.env.DATABASE_URL = pgContainer.getConnectionUri();

    seed = new Pool({ connectionString: pgContainer.getConnectionUri() });
    // The Aggregator owns this table; recreate its shape for the read-path test.
    await seed.query(`
      CREATE TABLE funnel_actor_steps (
        project_id    text        NOT NULL,
        actor_id      text        NOT NULL,
        event_type    text        NOT NULL,
        first_seen_at timestamptz NOT NULL,
        PRIMARY KEY (project_id, actor_id, event_type)
      )`);

    const step = (actor: string, type: string, at: string) =>
      seed.query(
        'INSERT INTO funnel_actor_steps (project_id, actor_id, event_type, first_seen_at) VALUES ($1,$2,$3,$4)',
        [PROJECT, actor, type, at],
      );

    // Steps funnel: game_start → level_complete → purchase
    // a1: full funnel, in order
    await step('a1', 'game_start', '2026-05-01T10:00:00Z');
    await step('a1', 'level_complete', '2026-05-01T10:05:00Z');
    await step('a1', 'purchase', '2026-05-02T09:00:00Z');
    // a2: reaches level_complete, no purchase
    await step('a2', 'game_start', '2026-05-01T11:00:00Z');
    await step('a2', 'level_complete', '2026-05-01T11:30:00Z');
    // a3: only the entry step
    await step('a3', 'game_start', '2026-05-03T08:00:00Z');
    // a4: did purchase BEFORE game_start (out of order) — counts only at step 1
    await step('a4', 'game_start', '2026-05-04T10:00:00Z');
    await step('a4', 'purchase', '2026-05-04T09:00:00Z');

    const moduleRef = await Test.createTestingModule({
      imports: [FunnelApiTestModule],
    }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    );
    await app.init();
  }, 120_000);

  afterAll(async () => {
    await app?.close();
    await seed?.end();
    await pgContainer?.stop();
  });

  function server(): Server {
    return app.getHttpServer() as Server;
  }

  const url = (steps: string) =>
    `/funnel?projectId=${PROJECT}&steps=${steps}&from=${encodeURIComponent(FROM)}&to=${encodeURIComponent(TO)}`;

  it('computes ordered per-step actor counts and cumulative conversion rates', async () => {
    const res = await request(server()).get(url('game_start,level_complete,purchase')).expect(200);

    // step1: a1,a2,a3,a4 = 4 · step2 (in order): a1,a2 = 2 · step3 (in order): a1 = 1
    expect(res.body).toEqual({
      projectId: PROJECT,
      from: FROM,
      to: TO,
      steps: [
        { step: 1, eventType: 'game_start', actors: 4, conversionRate: 1 },
        { step: 2, eventType: 'level_complete', actors: 2, conversionRate: 0.5 },
        { step: 3, eventType: 'purchase', actors: 1, conversionRate: 0.25 },
      ],
    });
  });

  it('400s on fewer than two steps', async () => {
    await request(server()).get(url('game_start')).expect(400);
  });

  it('400s on duplicate steps', async () => {
    await request(server()).get(url('game_start,game_start')).expect(400);
  });
});
