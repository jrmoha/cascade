import type { Server } from 'node:http';
import {
  ValidationPipe,
  type INestApplication,
  type INestMicroservice,
  type Type as NestType,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Transport, type MicroserviceOptions } from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Kafka, type Admin } from 'kafkajs';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RAW_EVENTS_TOPIC, type RawEvent } from '@cascade/contracts';
import { AppModule as CollectorAppModule } from '../../services/collector/src/app.module';
import { AppModule as IngestionAppModule } from '../../services/ingestion-processor/src/app.module';
import { AppModule as QueryAppModule } from '../../services/query-api/src/app.module';

// The walking-skeleton gate (KAN-20). Stands up real Kafka + Cassandra, boots
// all three services in-process against them, POSTs an event to the Collector
// and reads it back out of the Query API — proving the whole pipe end to end:
//
//   POST /collect → Kafka(raw-events) → Ingestion-Processor → Cassandra → GET /query
//
// No mocking of the broker or the DB, per CLAUDE.md. Set SKIP_INTEGRATION=1 to
// skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Walking-skeleton smoke (e2e)', () => {
  // The Ingestion-Processor's consumer groupId (matches its main.ts). NestJS's
  // ServerKafka appends a "-server" postfix to the configured groupId, so the
  // group that actually forms on the broker — and the one we must wait on — is
  // `${INGESTION_GROUP_ID}-server`.
  const INGESTION_GROUP_ID = 'cascade-ingestion-processor';
  const INGESTION_BROKER_GROUP = `${INGESTION_GROUP_ID}-server`;

  let kafka: StartedKafkaContainer;
  let cassandra: StartedTestContainer;
  let ingestion: INestMicroservice;
  let collector: INestApplication;
  let queryApi: INestApplication;

  beforeAll(async () => {
    // 1. Bring up the real infra the pipe runs on (in parallel — both are slow).
    [kafka, cassandra] = await Promise.all([
      new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start(),
      new GenericContainer('cassandra:4.1')
        .withExposedPorts(9042)
        .withStartupTimeout(180_000)
        .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
        .start(),
    ]);

    const broker = `${kafka.getHost()}:${kafka.getMappedPort(9093)}`;

    // Pre-create raw-events (single partition) so the consumer has a partition
    // to be assigned immediately rather than racing topic auto-creation.
    const admin: Admin = new Kafka({ clientId: 'smoke-admin', brokers: [broker] }).admin();
    await admin.connect();
    await admin.createTopics({ topics: [{ topic: RAW_EVENTS_TOPIC, numPartitions: 1 }] });

    // 2. Point every service at this infra (services read these env vars on boot).
    process.env.KAFKA_BOOTSTRAP_SERVERS = broker;
    process.env.CASSANDRA_CONTACT_POINTS = cassandra.getHost();
    process.env.CASSANDRA_PORT = String(cassandra.getMappedPort(9042));
    process.env.CASSANDRA_LOCAL_DC = 'datacenter1';

    // 3. Boot the Ingestion-Processor FIRST. It ensures the Cassandra schema on
    //    startup, and it must have joined the consumer group before we produce —
    //    its consumer reads from latest (fromBeginning=false), so an event
    //    produced before the group is assigned the partition would be skipped.
    ingestion = await NestFactory.createMicroservice<MicroserviceOptions>(IngestionAppModule, {
      transport: Transport.KAFKA,
      options: {
        client: { clientId: INGESTION_GROUP_ID, brokers: [broker] },
        consumer: { groupId: INGESTION_GROUP_ID },
      },
    });
    await ingestion.listen();
    await waitForGroupStable(admin, INGESTION_BROKER_GROUP);
    await admin.disconnect();

    // 4. Boot the Query API (reads the schema the processor just ensured).
    queryApi = await bootHttpApp(QueryAppModule);

    // 5. Boot the Collector (Kafka producer onto raw-events).
    collector = await bootHttpApp(CollectorAppModule);
  });

  afterAll(async () => {
    await collector?.close();
    await queryApi?.close();
    await ingestion?.close();
    await Promise.all([kafka?.stop(), cassandra?.stop()]);
  });

  it('round-trips an event: POST /collect → Kafka → Cassandra → GET /query', async () => {
    const sent = {
      projectId: 'game-1',
      type: 'level_complete',
      // Explicit occurredAt (event time) so the read-back assertion is exact;
      // the Collector stamps receivedAt (ingest time) itself.
      occurredAt: new Date().toISOString(),
      payload: { level: 7, score: 4200 },
      // Optional envelope fields — assert they persist all the way through.
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    };

    // Event in.
    const post = await request(collector.getHttpServer() as Server)
      .post('/collect')
      .send(sent)
      .expect(202);

    expect(post.body.status).toBe('accepted');
    const eventId = post.body.eventId as string;
    expect(eventId).toBeTruthy();

    // Event out — poll the read path until it has flowed all the way through.
    // hours=2 spans the current + previous hourly bucket (robust across rollover).
    const event = await waitFor(async () => {
      const res = await request(queryApi.getHttpServer() as Server)
        .get(`/query?projectId=${sent.projectId}&hours=2`)
        .expect(200);
      return (res.body.events as RawEvent[]).find((e) => e.eventId === eventId);
    });

    // receivedAt is stamped by the Collector (ingest time), so assert it is a
    // valid ISO timestamp rather than a fixed value, then check the rest of the
    // envelope round-tripped exactly.
    const { receivedAt, ...rest } = event as RawEvent;
    expect(Number.isNaN(Date.parse(receivedAt))).toBe(false);
    expect(rest).toEqual({
      eventId,
      projectId: sent.projectId,
      type: sent.type,
      occurredAt: sent.occurredAt,
      payload: sent.payload,
      sessionId: sent.sessionId,
      actorId: sent.actorId,
      source: sent.source,
    });
  });
});

/** Boot a NestJS HTTP service in-process with the same global pipe as production. */
async function bootHttpApp(module: NestType): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({ imports: [module] }).compile();
  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(
    new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
  );
  await app.init();
  return app;
}

/** Wait until a Kafka consumer group has joined and been assigned its partition. */
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

/** Poll an async probe until it returns a defined value, or time out. */
async function waitFor<T>(
  probe: () => Promise<T | undefined>,
  { timeoutMs = 30_000, intervalMs = 500 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await probe();
    if (result !== undefined) return result;
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for the event to flow through the pipe');
    }
    await sleep(intervalMs);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
