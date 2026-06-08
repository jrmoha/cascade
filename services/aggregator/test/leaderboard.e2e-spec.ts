import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Admin, Kafka, type Producer } from 'kafkajs';
import Redis from 'ioredis';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_DLQ_TOPIC, RAW_EVENTS_TOPIC, type RawEvent } from '@cascade/contracts';
import { AppConfigModule } from '../src/config/config.module';
import { CassandraModule } from '../src/cassandra/cassandra.module';
import { RedisModule } from '../src/redis/redis.module';
import { AggregationModule } from '../src/aggregation/aggregation.module';

// Integration test for the live-leaderboard read model (KAN-34). Boots the real
// Aggregator consumer wiring against real Kafka + Cassandra + Redis (the
// controller derives counts AND the leaderboard, so Cassandra is needed too),
// feeds a known sequence of score events, and asserts the Redis sorted set: top-N
// order, a specific player's rank, best-score semantics (a later LOWER score does
// not lower the player), and that both the all-time and daily boards are written
// with the daily board carrying a TTL. Set SKIP_INTEGRATION=1 to skip.

@Module({
  imports: [AppConfigModule, CassandraModule, RedisModule, AggregationModule],
})
class LeaderboardTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Aggregator live leaderboard (integration)',
  () => {
    const BROKER_GROUP = 'cascade-aggregator-server';
    const PROJECT = 'arena';
    const DAY = '2026-05-30';
    const ALLTIME_KEY = `lb:${PROJECT}:alltime`;
    const DAILY_KEY = `lb:${PROJECT}:${DAY}`;
    const DAILY_TTL = 172800;

    let kafka: StartedKafkaContainer;
    let cassandraContainer: StartedTestContainer;
    let redisContainer: StartedTestContainer;
    let aggregator: INestMicroservice;
    let producer: Producer;
    let read: Redis;

    beforeAll(async () => {
      [kafka, cassandraContainer, redisContainer] = await Promise.all([
        new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start(),
        new GenericContainer('cassandra:4.1')
          .withExposedPorts(9042)
          .withStartupTimeout(180_000)
          .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
          .start(),
        new GenericContainer('redis:7.2-alpine')
          .withExposedPorts(6379)
          .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
          .start(),
      ]);

      const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;
      const client = new Kafka({ clientId: 'leaderboard-test', brokers: [broker] });

      const admin: Admin = client.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: RAW_EVENTS_TOPIC, numPartitions: 1 },
          { topic: RAW_EVENTS_DLQ_TOPIC, numPartitions: 1 },
        ],
      });

      process.env.KAFKA_BOOTSTRAP_SERVERS = broker;
      process.env.CASSANDRA_CONTACT_POINTS = cassandraContainer.getHost();
      process.env.CASSANDRA_PORT = String(cassandraContainer.getMappedPort(9042));
      process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
      process.env.REDIS_HOST = redisContainer.getHost();
      process.env.REDIS_PORT = String(redisContainer.getMappedPort(6379));
      process.env.AGGREGATOR_DEDUP_TTL_SECONDS = '3600';
      process.env.AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS = String(DAILY_TTL);
      process.env.DATABASE_URL = 'postgresql://cascade:cascade@localhost:5432/cascade';

      read = new Redis({
        host: redisContainer.getHost(),
        port: redisContainer.getMappedPort(6379),
      });

      producer = client.producer();
      await producer.connect();

      aggregator = await NestFactory.createMicroservice<MicroserviceOptions>(
        LeaderboardTestModule,
        {
          transport: Transport.KAFKA,
          options: {
            client: { clientId: 'cascade-aggregator', brokers: [broker] },
            consumer: { groupId: 'cascade-aggregator' },
          },
        },
      );
      await aggregator.listen();
      await waitForGroupStable(admin, BROKER_GROUP);
      await admin.disconnect();
    }, 240_000);

    afterAll(async () => {
      await producer?.disconnect();
      await aggregator?.close();
      await read?.quit();
      await Promise.all([kafka?.stop(), cassandraContainer?.stop(), redisContainer?.stop()]);
    });

    it('maintains a best-score ranked board (top-N + rank) on the all-time and daily keys', async () => {
      const score = (playerId: string, value: number): RawEvent => ({
        eventId: randomUUID(),
        projectId: PROJECT,
        schemaVersion: 1,
        type: 'score',
        occurredAt: `${DAY}T12:00:00.000Z`,
        receivedAt: `${DAY}T12:00:00.000Z`,
        payload: { playerId, score: value },
      });
      // A non-score event (no playerId/score) — must be ignored by the board.
      const nonScore: RawEvent = {
        eventId: randomUUID(),
        projectId: PROJECT,
        schemaVersion: 1,
        type: 'level_complete',
        occurredAt: `${DAY}T12:00:00.000Z`,
        receivedAt: `${DAY}T12:00:00.000Z`,
        payload: { level: 4 },
      };

      const messages = [
        score('p1', 100),
        score('p2', 300),
        score('p3', 200),
        score('p1', 50), // LOWER than p1's 100 — best-score must keep 100
        score('p2', 350), // higher — raises p2
        nonScore,
        score('zzz', 1), // sentinel, produced last
      ].map((e) => ({ key: PROJECT, value: JSON.stringify(e) }));
      await producer.send({ topic: RAW_EVENTS_TOPIC, messages });

      // Single partition → once the sentinel lands, every earlier event is applied.
      await waitFor(async () =>
        (await read.zscore(ALLTIME_KEY, 'zzz')) === '1' ? true : undefined,
      );

      // Top-N order (highest first), best-score applied: p2=350, p3=200, p1=100, zzz=1.
      expect(await read.zrevrange(ALLTIME_KEY, 0, -1, 'WITHSCORES')).toEqual([
        'p2',
        '350',
        'p3',
        '200',
        'p1',
        '100',
        'zzz',
        '1',
      ]);
      // Top-2 only.
      expect(await read.zrevrange(ALLTIME_KEY, 0, 1)).toEqual(['p2', 'p3']);
      // A specific player's rank (0-based from Redis; the Query API serves it +1).
      expect(await read.zrevrank(ALLTIME_KEY, 'p1')).toBe(2);
      // Best-score: p1's later 50 did NOT lower the stored 100.
      expect(await read.zscore(ALLTIME_KEY, 'p1')).toBe('100');

      // The daily board mirrors the all-time ranking and self-expires (TTL set).
      expect(await read.zrevrange(DAILY_KEY, 0, -1, 'WITHSCORES')).toEqual([
        'p2',
        '350',
        'p3',
        '200',
        'p1',
        '100',
        'zzz',
        '1',
      ]);
      const dailyTtl = await read.ttl(DAILY_KEY);
      expect(dailyTtl).toBeGreaterThan(0);
      expect(dailyTtl).toBeLessThanOrEqual(DAILY_TTL);
      // The all-time board never expires.
      expect(await read.ttl(ALLTIME_KEY)).toBe(-1);
    });
  },
);

async function waitForGroupStable(
  admin: Admin,
  groupId: string,
  { timeoutMs = 60_000, intervalMs = 500 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const { groups } = await admin.describeGroups([groupId]);
    const group = groups.find((g) => g.groupId === groupId);
    if (group?.state === 'Stable' && group.members.length > 0) return;
    if (Date.now() > deadline) {
      throw new Error(`Consumer group "${groupId}" did not stabilise (state=${group?.state})`);
    }
    await sleep(intervalMs);
  }
}

async function waitFor<T>(
  probe: () => Promise<T | undefined>,
  { timeoutMs = 30_000, intervalMs = 500 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await probe();
    if (result !== undefined) return result;
    if (Date.now() > deadline) throw new Error('Timed out waiting for the leaderboard to converge');
    await sleep(intervalMs);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
