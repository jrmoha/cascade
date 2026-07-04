import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Admin, Kafka, type Producer } from 'kafkajs';
import { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_DLQ_TOPIC, RAW_EVENTS_TOPIC, type RawEvent } from '@cascade/contracts';
import { AppConfigModule } from '../src/config/config.module';
import { CassandraModule } from '../src/cassandra/cassandra.module';
import { RedisModule } from '../src/redis/redis.module';
import { PostgresModule } from '../src/postgres/postgres.module';
import { AggregationModule } from '../src/aggregation/aggregation.module';

// Integration test for the funnel & retention write path (KAN-35). Boots the real
// Aggregator consumer wiring against real Kafka + Cassandra + Redis + Postgres,
// feeds actor journeys, and asserts the Postgres summary tables the Query API
// later reads — including the KAN-33 idempotency contract: the LEAST/ON-CONFLICT
// upserts are naturally replay-safe, so re-delivery and out-of-order arrival
// converge to the same rows. Set SKIP_INTEGRATION=1 to skip.

@Module({
  imports: [AppConfigModule, CassandraModule, RedisModule, PostgresModule, AggregationModule],
})
class FunnelRetentionTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Aggregator funnel & retention (integration)',
  () => {
    const BROKER_GROUP = 'cascade-aggregator-server';
    const PROJECT = 'rpg';

    let kafka: StartedKafkaContainer;
    let cassandraContainer: StartedTestContainer;
    let redisContainer: StartedTestContainer;
    let postgresContainer: StartedPostgreSqlContainer;
    let aggregator: INestMicroservice;
    let producer: Producer;
    let pool: Pool;

    beforeAll(async () => {
      [kafka, cassandraContainer, redisContainer, postgresContainer] = await Promise.all([
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
        new PostgreSqlContainer('postgres:16-alpine').start(),
      ]);

      const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;
      const client = new Kafka({ clientId: 'funnel-retention-test', brokers: [broker] });

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
      process.env.AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS = '172800';
      process.env.DATABASE_URL = postgresContainer.getConnectionUri();

      pool = new Pool({ connectionString: postgresContainer.getConnectionUri() });

      producer = client.producer();
      await producer.connect();

      aggregator = await NestFactory.createMicroservice<MicroserviceOptions>(
        FunnelRetentionTestModule,
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
    }, 300_000);

    afterAll(async () => {
      await producer?.disconnect();
      await aggregator?.close();
      await pool?.end();
      await Promise.all([
        kafka?.stop(),
        cassandraContainer?.stop(),
        redisContainer?.stop(),
        postgresContainer?.stop(),
      ]);
    });

    /** Build a journey event with an actorId and event-time. */
    const ev = (actorId: string, type: string, occurredAt: string): RawEvent => ({
      eventId: randomUUID(),
      projectId: PROJECT,
      schemaVersion: 1,
      type,
      occurredAt,
      receivedAt: occurredAt,
      actorId,
      payload: {},
    });

    const send = (events: RawEvent[]) =>
      producer.send({
        topic: RAW_EVENTS_TOPIC,
        messages: events.map((e) => ({ key: PROJECT, value: JSON.stringify(e) })),
      });

    it('records per-actor first-seen steps (LEAST) and active days (set), replay-safe', async () => {
      const journey = [
        // a1 does the full funnel in order across two days.
        ev('a1', 'game_start', '2026-05-01T10:00:00.000Z'),
        ev('a1', 'level_complete', '2026-05-01T10:05:00.000Z'),
        ev('a1', 'purchase', '2026-05-02T09:00:00.000Z'),
        // a2 stops after level_complete.
        ev('a2', 'game_start', '2026-05-01T11:00:00.000Z'),
        ev('a2', 'level_complete', '2026-05-01T11:30:00.000Z'),
        // A LATE duplicate of a1's game_start with a LATER timestamp — LEAST must
        // keep the earliest (10:00), proving naturally-idempotent out-of-order.
        ev('a1', 'game_start', '2026-05-01T23:59:00.000Z'),
        // An event without any actor identity — must be ignored by both views.
        { ...ev('', 'game_start', '2026-05-01T10:00:00.000Z'), actorId: undefined },
      ];

      // Produce the whole journey TWICE (at-least-once) — replay must be a no-op.
      await send(journey);
      const sentinel = ev('zzz', 'sentinel', '2026-05-09T00:00:00.000Z');
      await send([...journey, sentinel]);

      await waitFor(async () => {
        const { rows } = await pool.query(
          'SELECT 1 FROM funnel_actor_steps WHERE project_id=$1 AND actor_id=$2 AND event_type=$3',
          [PROJECT, 'zzz', 'sentinel'],
        );
        return rows.length === 1 ? true : undefined;
      });

      // Funnel state: one row per (actor, type); a1's game_start kept its earliest
      // time despite the later duplicate. The actorless event left no row.
      const funnel = await pool.query<{
        actor_id: string;
        event_type: string;
        first_seen_at: Date;
      }>(
        `SELECT actor_id, event_type, first_seen_at FROM funnel_actor_steps
         WHERE project_id=$1 AND actor_id IN ('a1','a2')
         ORDER BY actor_id, event_type`,
        [PROJECT],
      );
      expect(funnel.rows.map((r) => [r.actor_id, r.event_type])).toEqual([
        ['a1', 'game_start'],
        ['a1', 'level_complete'],
        ['a1', 'purchase'],
        ['a2', 'game_start'],
        ['a2', 'level_complete'],
      ]);
      const a1Start = funnel.rows.find(
        (r) => r.actor_id === 'a1' && r.event_type === 'game_start',
      )!;
      expect(new Date(a1Start.first_seen_at).toISOString()).toBe('2026-05-01T10:00:00.000Z');

      // Retention state: set of active UTC days per actor (a1 on 05-01 and 05-02).
      // Format the `date` in SQL to avoid JS local-timezone shifting.
      const activity = await pool.query<{ actor_id: string; active_period: string }>(
        `SELECT actor_id, to_char(active_period, 'YYYY-MM-DD') AS active_period
         FROM retention_actor_activity
         WHERE project_id=$1 AND actor_id IN ('a1','a2')
         ORDER BY actor_id, active_period`,
        [PROJECT],
      );
      const days = activity.rows.map((r) => [r.actor_id, r.active_period]);
      expect(days).toEqual([
        ['a1', '2026-05-01'],
        ['a1', '2026-05-02'],
        ['a2', '2026-05-01'],
      ]);
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
    if (Date.now() > deadline) throw new Error('Timed out waiting for summaries to converge');
    await sleep(intervalMs);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
