import type { Server } from 'node:http';
import { Module, ValidationPipe, type INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import Redis from 'ioredis';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { AppConfigModule } from '../src/config/config.module';
import { RedisModule } from '../src/redis/redis.module';
import { LeaderboardModule } from '../src/leaderboard/leaderboard.module';

// Integration test for the Query API leaderboard endpoints against a real Redis
// (Testcontainers, no mocks per CLAUDE.md). Boots a minimal app — config + Redis
// + leaderboard only, no Cassandra — seeds a sorted set the way the Aggregator
// would, and asserts top-N ordering, a player's rank/score, and the 404 path.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

@Module({ imports: [AppConfigModule, RedisModule, LeaderboardModule] })
class LeaderboardApiTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Query API leaderboard (integration)', () => {
  const PROJECT = 'arena';
  const ALLTIME_KEY = `lb:${PROJECT}:alltime`;

  let redisContainer: StartedTestContainer;
  let app: INestApplication;
  let seed: Redis;

  beforeAll(async () => {
    redisContainer = await new GenericContainer('redis:7.2-alpine')
      .withExposedPorts(6379)
      .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
      .start();

    // Cassandra vars are required by the env schema but unused here (no CassandraModule).
    process.env.CASSANDRA_CONTACT_POINTS = 'localhost';
    process.env.CASSANDRA_PORT = '9042';
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
    process.env.REDIS_HOST = redisContainer.getHost();
    process.env.REDIS_PORT = String(redisContainer.getMappedPort(6379));

    seed = new Redis({ host: redisContainer.getHost(), port: redisContainer.getMappedPort(6379) });
    // Seed the board the way the Aggregator writes it (best score per player).
    await seed.zadd(ALLTIME_KEY, 350, 'p2', 200, 'p3', 100, 'p1');

    const moduleRef = await Test.createTestingModule({
      imports: [LeaderboardApiTestModule],
    }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    );
    await app.init();
  }, 120_000);

  afterAll(async () => {
    await app?.close();
    await seed?.quit();
    await redisContainer?.stop();
  });

  function server(): Server {
    return app.getHttpServer() as Server;
  }

  it('GET /leaderboard returns top-N highest-first with 1-based ranks', async () => {
    const res = await request(server()).get(`/leaderboard?projectId=${PROJECT}`).expect(200);
    expect(res.body).toEqual({
      projectId: PROJECT,
      period: 'alltime',
      entries: [
        { playerId: 'p2', score: 350, rank: 1 },
        { playerId: 'p3', score: 200, rank: 2 },
        { playerId: 'p1', score: 100, rank: 3 },
      ],
    });
  });

  it('GET /leaderboard honours the limit', async () => {
    const res = await request(server())
      .get(`/leaderboard?projectId=${PROJECT}&limit=2`)
      .expect(200);
    expect((res.body.entries as unknown[]).length).toBe(2);
    expect(res.body.entries[0]).toEqual({ playerId: 'p2', score: 350, rank: 1 });
  });

  it('GET /leaderboard/rank returns a player rank + score', async () => {
    const res = await request(server())
      .get(`/leaderboard/rank?projectId=${PROJECT}&playerId=p1`)
      .expect(200);
    expect(res.body).toEqual({
      projectId: PROJECT,
      period: 'alltime',
      playerId: 'p1',
      rank: 3,
      score: 100,
    });
  });

  it('GET /leaderboard/rank 404s for a player not on the board', async () => {
    await request(server())
      .get(`/leaderboard/rank?projectId=${PROJECT}&playerId=ghost`)
      .expect(404);
  });

  it('GET /leaderboard 400s on a malformed period', async () => {
    await request(server()).get(`/leaderboard?projectId=${PROJECT}&period=last-week`).expect(400);
  });
});
