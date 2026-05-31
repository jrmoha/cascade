import type { Server } from 'node:http';
import { ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { Consumer, Kafka } from 'kafkajs';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_TOPIC, RawEvent } from '@cascade/contracts';
import { AppModule } from '../src/app.module';

// Integration test against a real Kafka broker (no mocking the broker, per
// CLAUDE.md). Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('POST /collect (integration)', () => {
  let kafka: StartedKafkaContainer;
  let app: INestApplication;
  let consumer: Consumer;
  const received: { key: string; value: RawEvent }[] = [];

  beforeAll(async () => {
    kafka = await new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start();
    const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;
    process.env.KAFKA_BOOTSTRAP_SERVERS = broker;

    const client = new Kafka({ clientId: 'test-consumer', brokers: [broker] });
    const admin = client.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: RAW_EVENTS_TOPIC, numPartitions: 1 }] });
    await admin.disconnect();

    consumer = client.consumer({ groupId: 'collector-e2e' });
    await consumer.connect();
    await consumer.subscribe({ topic: RAW_EVENTS_TOPIC, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        received.push({
          key: message.key?.toString() ?? '',
          value: JSON.parse(message.value?.toString() ?? '{}') as RawEvent,
        });
      },
    });

    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    );
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    await consumer?.disconnect();
    await kafka?.stop();
  });

  const server = (): Server => app.getHttpServer();

  it('returns 202 and lands the event on raw-events keyed by projectId', async () => {
    const res = await request(server())
      .post('/collect')
      .send({ projectId: 'game-1', type: 'level_complete', payload: { level: 3 } })
      .expect(202);

    expect(res.body.status).toBe('accepted');
    expect(res.body.eventId).toBeTruthy();

    const msg = await waitFor(() => received.find((m) => m.value.eventId === res.body.eventId));
    expect(msg.key).toBe('game-1');
    expect(msg.value.projectId).toBe('game-1');
    expect(msg.value.type).toBe('level_complete');
    expect(msg.value.payload).toEqual({ level: 3 });
    expect(Number.isNaN(Date.parse(msg.value.occurredAt))).toBe(false);
    expect(Number.isNaN(Date.parse(msg.value.receivedAt))).toBe(false);
  });

  it('rejects an event missing a required field with a structured 400 and produces nothing', async () => {
    const countBefore = received.length;
    const res = await request(server())
      .post('/collect')
      .send({ type: 'level_complete' })
      .expect(400);

    expect(res.body.message).toBe('Event validation failed');
    expect(res.body.errors).toEqual(
      expect.arrayContaining([expect.objectContaining({ field: 'projectId' })]),
    );
    for (const e of res.body.errors) {
      expect(typeof e.field).toBe('string');
      expect(typeof e.reason).toBe('string');
    }

    await new Promise((r) => setTimeout(r, 1000));
    expect(received.length).toBe(countBefore);
  });

  it('rejects a wrong-typed required field with 400 and produces nothing', async () => {
    const countBefore = received.length;
    const res = await request(server())
      .post('/collect')
      .send({ projectId: 123, type: 'level_complete' })
      .expect(400);

    expect(res.body.errors).toEqual(
      expect.arrayContaining([expect.objectContaining({ field: 'projectId' })]),
    );

    await new Promise((r) => setTimeout(r, 1000));
    expect(received.length).toBe(countBefore);
  });

  it('ignores a client-supplied receivedAt and stamps it server-side', async () => {
    const clientReceivedAt = '2000-01-01T00:00:00.000Z';
    const res = await request(server())
      .post('/collect')
      .send({ projectId: 'game-2', type: 'level_start', receivedAt: clientReceivedAt })
      .expect(202);

    const msg = await waitFor(() => received.find((m) => m.value.eventId === res.body.eventId));
    expect(msg.value.receivedAt).not.toBe(clientReceivedAt);
    expect(Number.isNaN(Date.parse(msg.value.receivedAt))).toBe(false);
  });
});

async function waitFor<T>(
  predicate: () => T | undefined,
  { timeoutMs = 15_000, intervalMs = 200 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = predicate();
    if (result !== undefined) return result;
    if (Date.now() > deadline) throw new Error('Timed out waiting for Kafka message');
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
