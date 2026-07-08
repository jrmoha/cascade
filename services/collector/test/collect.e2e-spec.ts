import type { Server } from 'node:http';
import { Controller, Module, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import type { INestApplication, INestMicroservice } from '@nestjs/common';
import {
  GrpcMethod,
  RpcException,
  Transport,
  type MicroserviceOptions,
} from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Consumer, Kafka } from 'kafkajs';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  PROJECT_SCHEMA_GRPC_SERVICE,
  PROJECT_SCHEMA_PROTO_PACKAGE,
  PROJECT_SCHEMA_PROTO_PATH,
  RAW_EVENTS_TOPIC,
  RawEvent,
  projectSchemaProto,
} from '@cascade/contracts';

// Full ingest path (KAN-30): API-key auth + per-project schema validation, with
// the Redis cache and the Project/Schema gRPC dependency. Runs against a real
// Kafka, a real Redis, and an in-process gRPC stub standing in for
// Project/Schema (so we can assert cache hits and drive the fail-closed path by
// stopping it). The full real-Project/Schema flow is covered by the e2e smoke.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.

const VALID_KEY = 'cas_valid.secret';
const OTHER_KEY = 'cas_other.secret';
const PROJECT_ID = 'proj-1';
const STUB_ADDR = '127.0.0.1:50252';

const LEVEL_SCHEMA = {
  type: 'object',
  properties: { level: { type: 'integer' } },
  required: ['level'],
  additionalProperties: true,
};

// Call counters so we can prove a cache hit makes NO gRPC call.
let verifyCalls = 0;
let schemaCalls = 0;

/** In-process stand-in for Project/Schema's gRPC service. */
@Controller()
class StubProjectSchemaController {
  @GrpcMethod(PROJECT_SCHEMA_GRPC_SERVICE, 'VerifyKey')
  verifyKey(req: projectSchemaProto.VerifyKeyRequest): projectSchemaProto.VerifyKeyResponse {
    verifyCalls += 1;
    return req.key === VALID_KEY
      ? { valid: true, projectId: PROJECT_ID }
      : { valid: false, projectId: undefined };
  }

  @GrpcMethod(PROJECT_SCHEMA_GRPC_SERVICE, 'GetEventSchema')
  getEventSchema(req: projectSchemaProto.GetEventSchemaRequest): projectSchemaProto.EventSchema {
    schemaCalls += 1;
    if (req.projectId === PROJECT_ID && req.eventType === 'level_complete') {
      return {
        id: 'schema-1',
        projectId: PROJECT_ID,
        eventType: 'level_complete',
        jsonSchema: JSON.stringify(LEVEL_SCHEMA),
        createdAt: '2026-01-01T00:00:00.000Z',
        updatedAt: '2026-01-01T00:00:00.000Z',
      };
    }
    throw new RpcException({ code: status.NOT_FOUND, message: 'no schema' });
  }
}

@Module({ controllers: [StubProjectSchemaController] })
class StubProjectSchemaModule {}

describe.skipIf(process.env.SKIP_INTEGRATION === '1')('POST /collect (ingest integration)', () => {
  let kafka: StartedKafkaContainer;
  let redis: StartedTestContainer;
  let stub: INestMicroservice;
  let app: INestApplication;
  let consumer: Consumer;
  const received: { key: string; value: RawEvent }[] = [];

  beforeAll(async () => {
    [kafka, redis] = await Promise.all([
      new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start(),
      new GenericContainer('redis:7.2-alpine')
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage('Ready to accept connections'))
        .start(),
    ]);

    const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;

    const client = new Kafka({ clientId: 'test-consumer', brokers: [broker] });
    const admin = client.admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: RAW_EVENTS_TOPIC, numPartitions: 1 }] });
    await admin.disconnect();

    consumer = client.consumer({ groupId: 'collector-ingest-e2e' });
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

    // Boot the gRPC stub for Project/Schema.
    stub = await NestFactory.createMicroservice<MicroserviceOptions>(StubProjectSchemaModule, {
      transport: Transport.GRPC,
      options: {
        package: PROJECT_SCHEMA_PROTO_PACKAGE,
        protoPath: PROJECT_SCHEMA_PROTO_PATH,
        url: STUB_ADDR,
      },
    });
    await stub.listen();

    // Point the Collector at this infra, then boot it.
    process.env.KAFKA_BOOTSTRAP_SERVERS = broker;
    process.env.REDIS_HOST = redis.getHost();
    process.env.REDIS_PORT = String(redis.getMappedPort(6379));
    process.env.PROJECT_SCHEMA_GRPC_URL = STUB_ADDR;
    process.env.PROJECT_SCHEMA_CACHE_TTL_SECONDS = '60';

    const { AppModule } = await import('../src/app.module');
    const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    );
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    await stub?.close();
    await consumer?.disconnect();
    await Promise.all([kafka?.stop(), redis?.stop()]);
  });

  const server = (): Server => app.getHttpServer();
  const valid = () => request(server()).post('/collect').set('x-api-key', VALID_KEY);

  it('rejects a request with no API key (401) and produces nothing', async () => {
    const before = received.length;
    await request(server())
      .post('/collect')
      .send({ type: 'level_complete', payload: { level: 1 } })
      .expect(401);
    await new Promise((r) => setTimeout(r, 500));
    expect(received.length).toBe(before);
  });

  it('rejects an unknown API key (401)', async () => {
    await request(server())
      .post('/collect')
      .set('x-api-key', 'cas_nope.wrong')
      .send({ type: 'level_complete', payload: { level: 1 } })
      .expect(401);
  });

  it('rejects a bad envelope (missing type) with a structured 400', async () => {
    const res = await valid()
      .send({ payload: { level: 1 } })
      .expect(400);
    expect(res.body.message).toBe('Event validation failed');
    expect(res.body.errors).toEqual(
      expect.arrayContaining([expect.objectContaining({ field: 'type' })]),
    );
  });

  it('accepts a valid key + registered type + valid payload, landing it on raw-events', async () => {
    const res = await valid()
      .send({ type: 'level_complete', payload: { level: 7 } })
      .expect(202);
    expect(res.body.status).toBe('accepted');

    const msg = await waitFor(() => received.find((m) => m.value.eventId === res.body.eventId));
    // Partition key falls back to eventId here (no sessionId/actorId sent) — KAN-40.
    expect(msg.key).toBe(res.body.eventId);
    expect(msg.value.projectId).toBe(PROJECT_ID); // projectId derived from the API key, not the body
    expect(msg.value.type).toBe('level_complete');
    expect(msg.value.payload).toEqual({ level: 7 });
  });

  it('rejects a payload that violates the project schema with a structured 400', async () => {
    const res = await valid()
      .send({ type: 'level_complete', payload: { level: 'not-an-int' } })
      .expect(400);
    expect(res.body.errors).toEqual(
      expect.arrayContaining([expect.objectContaining({ field: 'level' })]),
    );
  });

  it('rejects an unregistered event type with 422', async () => {
    const res = await valid().send({ type: 'never_registered', payload: {} }).expect(422);
    expect(res.body.message).toBe('Event validation failed');
  });

  it('serves a warm key+schema from cache (no gRPC) but fails closed (503) on a cold miss when Project/Schema is down', async () => {
    // Warm the cache, then take Project/Schema offline.
    await valid()
      .send({ type: 'level_complete', payload: { level: 9 } })
      .expect(202);
    const verifyBefore = verifyCalls;
    const schemaBefore = schemaCalls;
    await stub.close();

    // Same key + type → served entirely from Redis, no new gRPC calls.
    await valid()
      .send({ type: 'level_complete', payload: { level: 10 } })
      .expect(202);
    expect(verifyCalls).toBe(verifyBefore);
    expect(schemaCalls).toBe(schemaBefore);

    // A brand-new (uncached) key cannot be resolved → fail-closed 503.
    await request(server())
      .post('/collect')
      .set('x-api-key', OTHER_KEY)
      .send({ type: 'level_complete', payload: { level: 1 } })
      .expect(503);
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
