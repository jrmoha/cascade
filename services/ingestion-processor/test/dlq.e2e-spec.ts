import type { INestMicroservice } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Admin, Kafka, type Consumer, type Producer } from 'kafkajs';
import { Client } from 'cassandra-driver';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  RAW_EVENTS_DLQ_TOPIC,
  RAW_EVENTS_TOPIC,
  toHourlyBucket,
  type DeadLetter,
  type RawEvent,
} from '@cascade/contracts';
import { AppModule } from '../src/app.module';

// Integration test for dead-letter handling (KAN-23): a poison message injected
// between valid ones must land in the DLQ while the valid messages are still
// persisted — the consumer must not crash or block the partition. Real Kafka +
// Cassandra (no mocks, per CLAUDE.md). Set SKIP_INTEGRATION=1 to skip.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Ingestion DLQ (integration)', () => {
  const INGESTION_BROKER_GROUP = 'cascade-ingestion-processor-server';
  const PROJECT = 'game-dlq';
  const now = new Date().toISOString();
  const bucket = toHourlyBucket(now);

  let kafka: StartedKafkaContainer;
  let cassandra: StartedTestContainer;
  let processor: INestMicroservice;
  let producer: Producer;
  let dlqConsumer: Consumer;
  let seed: Client;
  const deadLettered: DeadLetter[] = [];

  beforeAll(async () => {
    [kafka, cassandra] = await Promise.all([
      new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start(),
      new GenericContainer('cassandra:4.1')
        .withExposedPorts(9042)
        .withStartupTimeout(180_000)
        .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
        .start(),
    ]);

    const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;
    const client = new Kafka({ clientId: 'dlq-test', brokers: [broker] });

    const admin: Admin = client.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: RAW_EVENTS_TOPIC, numPartitions: 1 },
        { topic: RAW_EVENTS_DLQ_TOPIC, numPartitions: 1 },
      ],
    });

    process.env.KAFKA_BOOTSTRAP_SERVERS = broker;
    process.env.CASSANDRA_CONTACT_POINTS = cassandra.getHost();
    process.env.CASSANDRA_PORT = String(cassandra.getMappedPort(9042));
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';

    // A throwaway client to read rows back (the processor owns the schema/writes).
    seed = new Client({
      contactPoints: [cassandra.getHost()],
      protocolOptions: { port: cassandra.getMappedPort(9042) },
      localDataCenter: 'datacenter1',
    });

    // Collect everything that lands on the DLQ.
    dlqConsumer = client.consumer({ groupId: 'dlq-assert' });
    await dlqConsumer.connect();
    await dlqConsumer.subscribe({ topic: RAW_EVENTS_DLQ_TOPIC, fromBeginning: true });
    await dlqConsumer.run({
      eachMessage: async ({ message }) => {
        deadLettered.push(JSON.parse(message.value?.toString() ?? '{}') as DeadLetter);
      },
    });

    producer = client.producer();
    await producer.connect();

    // Boot the Ingestion-Processor against the same infra (it ensures the schema
    // and joins the consumer group on startup).
    processor = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
      transport: Transport.KAFKA,
      options: {
        client: { clientId: 'cascade-ingestion-processor', brokers: [broker] },
        consumer: { groupId: 'cascade-ingestion-processor' },
      },
    });
    await processor.listen();
    await waitForGroupStable(admin, INGESTION_BROKER_GROUP);
    await admin.disconnect();
    await seed.connect();
  });

  afterAll(async () => {
    await producer?.disconnect();
    await dlqConsumer?.disconnect();
    await processor?.close();
    await seed?.shutdown();
    await Promise.all([kafka?.stop(), cassandra?.stop()]);
  });

  it('routes a poison message to the DLQ while valid messages keep flowing', async () => {
    const event = (eventId: string): RawEvent => ({
      eventId,
      projectId: PROJECT,
      type: 'level_complete',
      occurredAt: now,
      receivedAt: now,
      payload: { level: 3 },
    });
    const valid1 = event('11111111-1111-4111-8111-111111111111');
    const valid2 = event('22222222-2222-4222-8222-222222222222');

    // Inject a poison (non-JSON) message between two valid ones, in order, on the
    // same partition (same key).
    await producer.send({
      topic: RAW_EVENTS_TOPIC,
      messages: [
        { key: PROJECT, value: JSON.stringify(valid1) },
        { key: PROJECT, value: 'this-is-not-json' },
        { key: PROJECT, value: JSON.stringify(valid2) },
      ],
    });

    // Both valid events are persisted (proves the poison didn't block the partition)...
    const rows = await waitFor(async () => {
      const rs = await seed.execute(
        'SELECT event_id FROM cascade.raw_events WHERE project_id = ? AND time_bucket = ?',
        [PROJECT, bucket],
        { prepare: true },
      );
      return rs.rows.length >= 2 ? rs.rows : undefined;
    });
    const ids = rows.map((r) => r.get('event_id').toString()).sort();
    expect(ids).toEqual([valid1.eventId, valid2.eventId]);

    // ...and the poison message is in the DLQ with replay context.
    const dl = await waitFor(async () => deadLettered.find((d) => d.error.kind === 'validation'));
    expect(dl.originalValue).toBe('this-is-not-json');
    expect(dl.attempts).toBe(1);
    expect(dl.originalEvent).toBeUndefined();
    expect(dl.source.topic).toBe(RAW_EVENTS_TOPIC);
    expect(typeof dl.failedAt).toBe('string');
  });
});

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
    if (Date.now() > deadline) throw new Error('Timed out waiting for the DLQ/persistence outcome');
    await sleep(intervalMs);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
