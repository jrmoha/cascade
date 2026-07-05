import { Module, type INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Admin, Kafka, type Producer } from 'kafkajs';
import { Client } from 'cassandra-driver';
import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_DLQ_TOPIC, RAW_EVENTS_TOPIC, type RawEvent } from '@cascade/contracts';
import { AppConfigModule } from '../src/config/config.module';
import { CassandraModule } from '../src/cassandra/cassandra.module';
import { RedisModule } from '../src/redis/redis.module';
import { AggregationModule } from '../src/aggregation/aggregation.module';

// The keystone idempotency/replayability test for the Aggregator (KAN-33). Where
// `event-counts.e2e-spec.ts` proves a redelivered eventId is counted once, this
// file proves the three guarantees the keystone ticket is actually about, against
// real Kafka + Cassandra + Redis (no mocks, per CLAUDE.md):
//
//   A) Out-of-order DISTINCT events land in their EVENT-TIME bucket, regardless of
//      arrival order (AC: "duplicates AND out-of-order").
//   B) A full replay from offset 0 into clean tables reproduces aggregates byte-for
//      -byte identical to the original pass — the rebuildability guarantee (AC:
//      "replaying raw-events from offset 0 reproduces identical aggregates").
//   C) Redelivery across a consumer restart is a no-op: a clean close commits
//      offsets, a restart does not reprocess them, and re-produced duplicates are
//      deduped because the Redis dedup state survives the restart (AC: "no lost
//      updates / no double-count on crash").
//
// The topic has a single partition so global produce order is a total order: once
// a last-produced sentinel is counted, every earlier message has been processed.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

/** Minimal Aggregator wiring for the counts path — mirrors `event-counts.e2e-spec.ts`. */
@Module({
  imports: [AppConfigModule, CassandraModule, RedisModule, AggregationModule],
})
class CountsTestModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Aggregator idempotency & replayability (integration)',
  () => {
    // NestJS ServerKafka postfixes the consumer groupId with `-server` (see CLAUDE.md).
    const LIVE_GROUP = 'cascade-aggregator';
    const LIVE_BROKER_GROUP = `${LIVE_GROUP}-server`;
    const REBUILD_GROUP = 'cascade-aggregator-rebuild';
    const REBUILD_BROKER_GROUP = `${REBUILD_GROUP}-server`;

    let kafka: StartedKafkaContainer;
    let cassandraContainer: StartedTestContainer;
    let redisContainer: StartedTestContainer;
    let aggregator: INestMicroservice;
    let producer: Producer;
    let admin: Admin;
    let read: Client;
    let redis: Redis;
    let broker: string;

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

      broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;
      const client = new Kafka({ clientId: 'idempotency-test', brokers: [broker] });

      admin = client.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          // One partition → total ordering, so a sentinel marks full progress.
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
      // Required by the env schema but unused by the counts path (no Postgres wired).
      process.env.DATABASE_URL = 'postgresql://cascade:cascade@localhost:5432/cascade';

      // Throwaway clients to read counters back / drive the rebuild (the Aggregator owns writes).
      read = new Client({
        contactPoints: [cassandraContainer.getHost()],
        protocolOptions: { port: cassandraContainer.getMappedPort(9042) },
        localDataCenter: 'datacenter1',
      });
      redis = new Redis({
        host: redisContainer.getHost(),
        port: redisContainer.getMappedPort(6379),
      });

      producer = client.producer();
      await producer.connect();

      aggregator = await bootConsumer(LIVE_GROUP);
      await waitForGroupStable(admin, LIVE_BROKER_GROUP);
      await read.connect();
    }, 240_000);

    afterAll(async () => {
      await producer?.disconnect();
      await aggregator?.close();
      await read?.shutdown();
      await redis?.quit();
      await admin?.disconnect();
      await Promise.all([kafka?.stop(), cassandraContainer?.stop(), redisContainer?.stop()]);
    });

    // A) OUT-OF-ORDER DISTINCT EVENTS -------------------------------------------------
    it('buckets out-of-order distinct events by event time, not arrival order', async () => {
      const project = 'ooo';
      // Produced in DESCENDING event-time order: each later message happened EARLIER.
      // A correct event-time aggregator must still place each in its own bucket.
      const messages = [
        ev(project, 'level_complete', '2026-05-30T15:17:30.000Z'), // hour 15, min 15:17
        ev(project, 'level_complete', '2026-05-30T15:16:30.000Z'), // late by a minute
        ev(project, 'level_complete', '2026-05-30T14:59:00.000Z'), // late by an hour
        ev(project, 'purchase', '2026-05-30T15:16:05.000Z'), // late, different type
        sentinel(project, '2026-05-30T15:18:00.000Z'),
      ];
      await produce(project, messages);
      await waitForCount('event_counts_by_minute', project, '2026-05-30T15:18', 'session_start', 1);

      // Each distinct event sits in the minute it HAPPENED, despite reversed arrival.
      expect(
        await countOf('event_counts_by_minute', project, '2026-05-30T15:17', 'level_complete'),
      ).toBe(1);
      expect(
        await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'level_complete'),
      ).toBe(1);
      expect(
        await countOf('event_counts_by_minute', project, '2026-05-30T14:59', 'level_complete'),
      ).toBe(1);
      expect(await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'purchase')).toBe(
        1,
      );
      // And rolls up into the correct hour buckets (two distinct hours: 14 and 15).
      expect(
        await countOf('event_counts_by_hour', project, '2026-05-30T15', 'level_complete'),
      ).toBe(2);
      expect(
        await countOf('event_counts_by_hour', project, '2026-05-30T14', 'level_complete'),
      ).toBe(1);
      expect(await countOf('event_counts_by_hour', project, '2026-05-30T15', 'purchase')).toBe(1);
    });

    // C) REDELIVERY ACROSS A CONSUMER RESTART -----------------------------------------
    it('does not double-count or lose updates across a consumer restart', async () => {
      const project = 'restart';
      const batch = [
        ev(project, 'level_complete', '2026-05-30T15:16:10.000Z'),
        ev(project, 'level_complete', '2026-05-30T15:16:20.000Z'),
        ev(project, 'purchase', '2026-05-30T15:16:30.000Z'),
      ];
      await produce(project, [...batch, sentinel(project, '2026-05-30T15:16:59.000Z')]);
      await waitForCount('event_counts_by_minute', project, '2026-05-30T15:16', 'session_start', 1);

      const before = {
        lc: await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'level_complete'),
        p: await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'purchase'),
      };
      expect(before).toEqual({ lc: 2, p: 1 });

      // Clean close commits offsets; a fresh consumer on the SAME group resumes from
      // them and does NOT reprocess the committed batch.
      await aggregator.close();
      aggregator = await bootConsumer(LIVE_GROUP);
      await waitForGroupStable(admin, LIVE_BROKER_GROUP);

      // Re-produce the SAME events (a duplicate delivery) plus a NEW progress sentinel.
      // Redis dedup state survived the restart, so the duplicates are no-ops.
      await produce(project, [...batch, sentinel(project, '2026-05-30T15:17:00.000Z')]);
      await waitForCount('event_counts_by_minute', project, '2026-05-30T15:17', 'session_start', 1);

      expect(
        await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'level_complete'),
      ).toBe(before.lc);
      expect(await countOf('event_counts_by_minute', project, '2026-05-30T15:16', 'purchase')).toBe(
        before.p,
      );
    });

    // B) REBUILD FROM OFFSET 0 ≡ THE ORIGINAL PASS ------------------------------------
    // Runs LAST: it TRUNCATEs the shared counter tables, so it must observe the full
    // accumulated state from the earlier cases and reproduce ALL of it.
    it('reproduces identical aggregates when replayed from offset 0 into clean tables', async () => {
      // A final global sentinel: once it is counted, the whole log is materialised.
      const rebuildSentinel = sentinel('rebuild', '2026-05-30T15:20:00.000Z');
      await produce('rebuild', [rebuildSentinel]);
      await waitForCount(
        'event_counts_by_minute',
        'rebuild',
        '2026-05-30T15:20',
        'session_start',
        1,
      );

      const original = {
        minute: await snapshot('event_counts_by_minute'),
        hour: await snapshot('event_counts_by_hour'),
      };

      // Simulate the documented rebuild: clean the view store + the dedup state, then
      // replay the entire topic from offset 0 with a FRESH consumer group.
      await read.execute('TRUNCATE cascade.event_counts_by_minute');
      await read.execute('TRUNCATE cascade.event_counts_by_hour');
      await redis.flushall();

      const rebuilder = await bootConsumer(REBUILD_GROUP, { fromBeginning: true });
      await waitForGroupStable(admin, REBUILD_BROKER_GROUP);
      try {
        // Sentinel is the last offset on the single partition → its reappearance means
        // every earlier event has been re-applied.
        await waitForCount(
          'event_counts_by_minute',
          'rebuild',
          '2026-05-30T15:20',
          'session_start',
          1,
        );
        const rebuilt = {
          minute: await snapshot('event_counts_by_minute'),
          hour: await snapshot('event_counts_by_hour'),
        };
        expect(rebuilt.minute).toEqual(original.minute);
        expect(rebuilt.hour).toEqual(original.hour);
      } finally {
        await rebuilder.close();
      }
    });

    // --- helpers ---------------------------------------------------------------------

    function ev(projectId: string, type: string, occurredAt: string): RawEvent {
      return {
        eventId: randomUUID(),
        projectId,
        schemaVersion: 1,
        type,
        occurredAt,
        receivedAt: occurredAt,
        payload: {},
      };
    }

    /** A unique session_start used purely as a per-project progress marker. */
    function sentinel(projectId: string, occurredAt: string): RawEvent {
      return ev(projectId, 'session_start', occurredAt);
    }

    async function produce(key: string, events: RawEvent[]): Promise<void> {
      await producer.send({
        topic: RAW_EVENTS_TOPIC,
        messages: events.map((e) => ({ key, value: JSON.stringify(e) })),
      });
    }

    async function bootConsumer(
      groupId: string,
      subscribe?: { fromBeginning: boolean },
    ): Promise<INestMicroservice> {
      const app = await NestFactory.createMicroservice<MicroserviceOptions>(CountsTestModule, {
        transport: Transport.KAFKA,
        options: {
          client: { clientId: 'cascade-aggregator', brokers: [broker] },
          consumer: { groupId },
          ...(subscribe ? { subscribe } : {}),
        },
      });
      await app.listen();
      return app;
    }

    /** Read one counter back; an absent row means the counter is 0. */
    async function countOf(
      table: string,
      projectId: string,
      timeBucket: string,
      eventType: string,
    ): Promise<number> {
      const rs = await read.execute(
        `SELECT count FROM cascade.${table} WHERE project_id = ? AND time_bucket = ? AND event_type = ?`,
        [projectId, timeBucket, eventType],
        { prepare: true },
      );
      const row = rs.first();
      return row ? Number(row.get('count')) : 0;
    }

    async function waitForCount(
      table: string,
      projectId: string,
      timeBucket: string,
      eventType: string,
      expected: number,
    ): Promise<void> {
      await waitFor(async () =>
        (await countOf(table, projectId, timeBucket, eventType)) === expected ? true : undefined,
      );
    }

    /** Full-table snapshot as `project|bucket|type -> count`, for exact comparison. */
    async function snapshot(table: string): Promise<Record<string, number>> {
      const rs = await read.execute(
        `SELECT project_id, time_bucket, event_type, count FROM cascade.${table}`,
      );
      const out: Record<string, number> = {};
      for (const row of rs.rows) {
        const key = `${row.get('project_id')}|${row.get('time_bucket')}|${row.get('event_type')}`;
        out[key] = Number(row.get('count'));
      }
      return out;
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
