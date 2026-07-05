import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Admin, Kafka, type Producer } from 'kafkajs';
import { Client } from 'cassandra-driver';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_DLQ_TOPIC, RAW_EVENTS_TOPIC, type RawEvent } from '@cascade/contracts';
import { AppConfigModule } from '../src/config/config.module';
import { CassandraModule } from '../src/cassandra/cassandra.module';
import { RedisModule } from '../src/redis/redis.module';
import { AggregationModule } from '../src/aggregation/aggregation.module';

// Focused integration test for the windowed event-count read models (KAN-32).
// It boots the real Aggregator consumer wiring (config + Cassandra + Redis +
// aggregation) against real Kafka + Cassandra + Redis (no mocks, per CLAUDE.md),
// feeds a KNOWN set of events — including a duplicate eventId — and asserts the
// resulting per-minute and per-hour counters match EXACTLY, with the duplicate
// counted once (proving dedup makes redelivery a no-op). Postgres is not wired
// here (counts live only in Cassandra), so no Postgres container is needed.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

/**
 * Minimal Aggregator module for the counts path: everything the consumer needs
 * to derive counters, minus the HTTP/health server and the Postgres store
 * (unused by this view). Mirrors how `main.ts` boots, but trimmed to the view.
 */
@Module({
  imports: [AppConfigModule, CassandraModule, RedisModule, AggregationModule],
})
class CountsTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Aggregator event counts (integration)',
  () => {
    // NestJS ServerKafka postfixes the consumer groupId with `-server` (see CLAUDE.md).
    const BROKER_GROUP = 'cascade-aggregator-server';
    const PROJECT = 'game-counts';

    let kafka: StartedKafkaContainer;
    let cassandraContainer: StartedTestContainer;
    let redisContainer: StartedTestContainer;
    let aggregator: INestMicroservice;
    let producer: Producer;
    let read: Client;

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
      const client = new Kafka({ clientId: 'counts-test', brokers: [broker] });

      const admin: Admin = client.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: RAW_EVENTS_TOPIC, numPartitions: 1 },
          { topic: RAW_EVENTS_DLQ_TOPIC, numPartitions: 1 },
        ],
      });

      // The Aggregator reads its config from env (parsed once at boot).
      process.env.KAFKA_BOOTSTRAP_SERVERS = broker;
      process.env.CASSANDRA_CONTACT_POINTS = cassandraContainer.getHost();
      process.env.CASSANDRA_PORT = String(cassandraContainer.getMappedPort(9042));
      process.env.CASSANDRA_LOCAL_DC = 'datacenter1';
      process.env.CASSANDRA_REPLICATION_FACTOR = '1';
      process.env.CASSANDRA_CONSISTENCY = 'local_quorum';
      process.env.REDIS_HOST = redisContainer.getHost();
      process.env.REDIS_PORT = String(redisContainer.getMappedPort(6379));
      process.env.AGGREGATOR_DEDUP_TTL_SECONDS = '3600';
      process.env.AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS = '172800';
      // Required by the env schema but unused by the counts path (no Postgres wired).
      process.env.DATABASE_URL = 'postgresql://cascade:cascade@localhost:5432/cascade';

      // A throwaway client to read counter rows back (the Aggregator owns writes).
      read = new Client({
        contactPoints: [cassandraContainer.getHost()],
        protocolOptions: { port: cassandraContainer.getMappedPort(9042) },
        localDataCenter: 'datacenter1',
      });

      producer = client.producer();
      await producer.connect();

      // Boot the Aggregator consumer wiring; onApplicationBootstrap connects the
      // stores and runs the Cassandra migrator (creates the counter tables).
      aggregator = await NestFactory.createMicroservice<MicroserviceOptions>(CountsTestModule, {
        transport: Transport.KAFKA,
        options: {
          client: { clientId: 'cascade-aggregator', brokers: [broker] },
          consumer: { groupId: 'cascade-aggregator' },
        },
      });
      await aggregator.listen();
      await waitForGroupStable(admin, BROKER_GROUP);
      await admin.disconnect();
      await read.connect();
    }, 240_000);

    afterAll(async () => {
      await producer?.disconnect();
      await aggregator?.close();
      await read?.shutdown();
      await Promise.all([kafka?.stop(), cassandraContainer?.stop(), redisContainer?.stop()]);
    });

    it('maintains exact per-minute and per-hour counters, deduping a redelivered event', async () => {
      const event = (eventId: string, type: string, occurredAt: string): RawEvent => ({
        eventId,
        projectId: PROJECT,
        schemaVersion: 1,
        type,
        occurredAt,
        receivedAt: occurredAt,
        payload: {},
      });

      // Known event set, all on one partition (same key) so order is deterministic.
      //   minute 15:16 — level_complete x3, purchase x1
      //   minute 15:17 — level_complete x2
      //   minute 16:05 — level_complete x1   (second hour)
      //   minute 16:10 — session_start x1    (sentinel, produced last)
      const lc1 = event(
        '11111111-1111-4111-8111-111111111111',
        'level_complete',
        '2026-05-30T15:16:05.000Z',
      );
      const lc2 = event(
        '22222222-2222-4222-8222-222222222222',
        'level_complete',
        '2026-05-30T15:16:30.000Z',
      );
      const lc3 = event(
        '33333333-3333-4333-8333-333333333333',
        'level_complete',
        '2026-05-30T15:16:59.000Z',
      );
      const lc4 = event(
        '44444444-4444-4444-8444-444444444444',
        'level_complete',
        '2026-05-30T15:17:10.000Z',
      );
      const lc5 = event(
        '55555555-5555-4555-8555-555555555555',
        'level_complete',
        '2026-05-30T15:17:40.000Z',
      );
      const p1 = event(
        '66666666-6666-4666-8666-666666666666',
        'purchase',
        '2026-05-30T15:16:45.000Z',
      );
      const lc6 = event(
        '77777777-7777-4777-8777-777777777777',
        'level_complete',
        '2026-05-30T16:05:00.000Z',
      );
      const sentinel = event(
        '99999999-9999-4999-8999-999999999999',
        'session_start',
        '2026-05-30T16:10:00.000Z',
      );

      const messages = [lc1, lc2, lc3, lc4, lc5, p1, lc6, lc1 /* duplicate */, sentinel].map(
        (e) => ({
          key: PROJECT,
          value: JSON.stringify(e),
        }),
      );
      await producer.send({ topic: RAW_EVENTS_TOPIC, messages });

      // The sentinel is produced last; once it is counted, every earlier message —
      // including the duplicate — has been processed (single partition, in order).
      await waitFor(async () =>
        (await countOf('event_counts_by_minute', '2026-05-30T16:10', 'session_start')) === 1
          ? true
          : undefined,
      );

      // Per-minute counters — exact.
      expect(await countOf('event_counts_by_minute', '2026-05-30T15:16', 'level_complete')).toBe(3);
      expect(await countOf('event_counts_by_minute', '2026-05-30T15:17', 'level_complete')).toBe(2);
      expect(await countOf('event_counts_by_minute', '2026-05-30T15:16', 'purchase')).toBe(1);
      expect(await countOf('event_counts_by_minute', '2026-05-30T16:05', 'level_complete')).toBe(1);
      expect(await countOf('event_counts_by_minute', '2026-05-30T16:10', 'session_start')).toBe(1);

      // Per-hour counters — exact (15:16 + 15:17 roll up into hour 15).
      expect(await countOf('event_counts_by_hour', '2026-05-30T15', 'level_complete')).toBe(5);
      expect(await countOf('event_counts_by_hour', '2026-05-30T15', 'purchase')).toBe(1);
      expect(await countOf('event_counts_by_hour', '2026-05-30T16', 'level_complete')).toBe(1);
      expect(await countOf('event_counts_by_hour', '2026-05-30T16', 'session_start')).toBe(1);
    });

    /** Read one counter value back; an absent row means the counter is 0. */
    async function countOf(table: string, timeBucket: string, eventType: string): Promise<number> {
      const rs = await read.execute(
        `SELECT count FROM cascade.${table} WHERE project_id = ? AND time_bucket = ? AND event_type = ?`,
        [PROJECT, timeBucket, eventType],
        { prepare: true },
      );
      const row = rs.first();
      return row ? Number(row.get('count')) : 0;
    }
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
    if (Date.now() > deadline) throw new Error('Timed out waiting for counters to converge');
    await sleep(intervalMs);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
