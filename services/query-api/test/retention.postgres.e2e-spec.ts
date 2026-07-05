import type { Server } from 'node:http';
import { Module, ValidationPipe, type INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppConfigModule } from '../src/config/config.module';
import { PostgresModule } from '../src/postgres/postgres.module';
import { RetentionModule } from '../src/retention/retention.module';

// Integration test for the Query API retention endpoint against a real Postgres
// (Testcontainers, no mocks per CLAUDE.md). Seeds the Aggregator-owned
// `retention_actor_activity` table with two cohorts' active days, then asserts
// the derived cohort matrix (cohort = earliest active day; offset N = returners N
// days later). Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

@Module({ imports: [AppConfigModule, PostgresModule, RetentionModule] })
class RetentionApiTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Query API retention (integration)', () => {
  const PROJECT = 'rpg';

  let pgContainer: StartedPostgreSqlContainer;
  let app: INestApplication;
  let seed: Pool;

  beforeAll(async () => {
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();

    process.env.CASSANDRA_CONTACT_POINTS = 'localhost';
    process.env.CASSANDRA_PORT = '9042';
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
    process.env.CASSANDRA_REPLICATION_FACTOR = '1';
    process.env.CASSANDRA_CONSISTENCY = 'local_quorum';
    process.env.REDIS_HOST = 'localhost';
    process.env.REDIS_PORT = '6379';
    process.env.DATABASE_URL = pgContainer.getConnectionUri();

    seed = new Pool({ connectionString: pgContainer.getConnectionUri() });
    await seed.query(`
      CREATE TABLE retention_actor_activity (
        project_id    text NOT NULL,
        actor_id      text NOT NULL,
        active_period date NOT NULL,
        PRIMARY KEY (project_id, actor_id, active_period)
      )`);

    const active = (actor: string, day: string) =>
      seed.query(
        'INSERT INTO retention_actor_activity (project_id, actor_id, active_period) VALUES ($1,$2,$3)',
        [PROJECT, actor, day],
      );

    // Cohort 2026-05-01: a1 (days 0,1,2), a2 (days 0,2), a3 (day 0)
    await active('a1', '2026-05-01');
    await active('a1', '2026-05-02');
    await active('a1', '2026-05-03');
    await active('a2', '2026-05-01');
    await active('a2', '2026-05-03');
    await active('a3', '2026-05-01');
    // Cohort 2026-05-02: a4 (days 0,1)
    await active('a4', '2026-05-02');
    await active('a4', '2026-05-03');

    const moduleRef = await Test.createTestingModule({
      imports: [RetentionApiTestModule],
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

  it('computes the cohort retention matrix (cohort = earliest active day)', async () => {
    const res = await request(server())
      .get(`/retention?projectId=${PROJECT}&from=2026-05-01&to=2026-05-02&maxOffset=3`)
      .expect(200);

    expect(res.body).toEqual({
      projectId: PROJECT,
      granularity: 'day',
      cohorts: [
        {
          cohort: '2026-05-01',
          cohortSize: 3,
          offsets: [
            { offset: 0, actors: 3 },
            { offset: 1, actors: 1 },
            { offset: 2, actors: 2 },
          ],
        },
        {
          cohort: '2026-05-02',
          cohortSize: 1,
          offsets: [
            { offset: 0, actors: 1 },
            { offset: 1, actors: 1 },
          ],
        },
      ],
    });
  });

  it('400s when `from` is after `to`', async () => {
    await request(server())
      .get(`/retention?projectId=${PROJECT}&from=2026-05-10&to=2026-05-01`)
      .expect(400);
  });

  it('400s on a malformed date', async () => {
    await request(server())
      .get(`/retention?projectId=${PROJECT}&from=last-week&to=2026-05-01`)
      .expect(400);
  });
});
