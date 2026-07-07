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
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { Kafka, type Admin } from 'kafkajs';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  PROJECT_SCHEMA_PROTO_PACKAGE,
  PROJECT_SCHEMA_PROTO_PATH,
  RAW_EVENT_SCHEMA_VERSION,
  RAW_EVENTS_TOPIC,
  type RawEvent,
} from '@cascade/contracts';
import { AppModule as CollectorAppModule } from '../../services/collector/src/app.module';
import { AppModule as IngestionAppModule } from '../../services/ingestion-processor/src/app.module';
import { AppModule as ProjectSchemaAppModule } from '../../services/project-schema/src/app.module';
import { AppModule as QueryAppModule } from '../../services/query-api/src/app.module';

// The walking-skeleton gate (KAN-20), extended for the Phase-2 ingest loop
// (KAN-30). Stands up real Kafka + Cassandra + Postgres + Redis, boots all four
// services in-process, authenticates with a real API key + a registered schema,
// POSTs an event to the Collector and reads it back out of the Query API —
// proving the whole pipe end to end:
//
//   POST /collect (x-api-key) → [Project/Schema verify+validate, Redis-cached]
//     → Kafka(raw-events) → Ingestion-Processor → Cassandra → GET /query
//
// No mocking of the broker or the DBs, per CLAUDE.md. Set SKIP_INTEGRATION=1 to
// skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Walking-skeleton smoke (e2e)', () => {
  // The Ingestion-Processor's consumer groupId (matches its main.ts). NestJS's
  // ServerKafka appends a "-server" postfix to the configured groupId, so the
  // group that actually forms on the broker — and the one we must wait on — is
  // `${INGESTION_GROUP_ID}-server`.
  const INGESTION_GROUP_ID = 'cascade-ingestion-processor';
  const INGESTION_BROKER_GROUP = `${INGESTION_GROUP_ID}-server`;
  const PROJECT_SCHEMA_GRPC_ADDR = '127.0.0.1:50351';

  let kafka: StartedKafkaContainer;
  let cassandra: StartedTestContainer;
  let postgres: StartedPostgreSqlContainer;
  let redis: StartedTestContainer;
  let ingestion: INestMicroservice;
  let projectSchema: INestApplication;
  let collector: INestApplication;
  let queryApi: INestApplication;

  // Seeded in beforeAll: the authenticated project and its API key.
  let projectId: string;
  let apiKey: string;

  beforeAll(async () => {
    // 1. Bring up the real infra the pipe runs on (in parallel — all are slow).
    [kafka, cassandra, postgres, redis] = await Promise.all([
      new KafkaContainer('confluentinc/cp-kafka:7.5.0').withKraft().start(),
      new GenericContainer('cassandra:4.1')
        .withExposedPorts(9042)
        .withStartupTimeout(180_000)
        .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
        .start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
      new GenericContainer('redis:7.2-alpine')
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage('Ready to accept connections'))
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
    process.env.CASSANDRA_REPLICATION_FACTOR = '1';
    process.env.CASSANDRA_CONSISTENCY = 'local_quorum';
    process.env.DATABASE_URL = postgres.getConnectionUri();
    process.env.GRPC_URL = PROJECT_SCHEMA_GRPC_ADDR;
    process.env.REDIS_HOST = redis.getHost();
    process.env.REDIS_PORT = String(redis.getMappedPort(6379));
    process.env.PROJECT_SCHEMA_GRPC_URL = PROJECT_SCHEMA_GRPC_ADDR;

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

    // 5. Boot Project/Schema (hybrid HTTP + gRPC) and seed a project, an API key
    //    and the event schema the Collector will authenticate + validate against.
    projectSchema = await bootProjectSchema();
    const psHttp = request(projectSchema.getHttpServer() as Server);
    const project = await psHttp.post('/projects').send({ name: 'Smoke' }).expect(201);
    projectId = project.body.id as string;
    const issued = await psHttp.post(`/projects/${projectId}/keys`).expect(201);
    apiKey = issued.body.key as string;
    await psHttp
      .post(`/projects/${projectId}/schemas`)
      .send({
        eventType: 'level_complete',
        jsonSchema: {
          type: 'object',
          properties: { level: { type: 'integer' }, score: { type: 'integer' } },
          required: ['level'],
          additionalProperties: true,
        },
      })
      .expect(201);

    // 6. Boot the Collector (Kafka producer + the Project/Schema gRPC client).
    collector = await bootHttpApp(CollectorAppModule);
  });

  afterAll(async () => {
    await collector?.close();
    await projectSchema?.close();
    await queryApi?.close();
    await ingestion?.close();
    await Promise.all([kafka?.stop(), cassandra?.stop(), postgres?.stop(), redis?.stop()]);
  });

  it('round-trips an event: POST /collect (authenticated) → Kafka → Cassandra → GET /query', async () => {
    const sent = {
      // No projectId in the body — it is derived from the API key (KAN-30).
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

    // Event in — authenticated with the seeded API key; the payload is validated
    // against the registered `level_complete` schema before it reaches Kafka.
    const post = await request(collector.getHttpServer() as Server)
      .post('/collect')
      .set('x-api-key', apiKey)
      .send(sent)
      .expect(202);

    expect(post.body.status).toBe('accepted');
    const eventId = post.body.eventId as string;
    expect(eventId).toBeTruthy();

    // Event out — poll the read path until it has flowed all the way through.
    // Query a window from an hour before to just after now: spans the current +
    // previous hourly bucket (robust across rollover) and brackets occurredAt.
    const from = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const to = new Date(Date.now() + 60 * 1000).toISOString();
    const event = await waitFor(async () => {
      const res = await request(queryApi.getHttpServer() as Server)
        .get(`/query?projectId=${projectId}&from=${from}&to=${to}`)
        .expect(200);
      return (res.body.events as RawEvent[]).find((e) => e.eventId === eventId);
    });

    // receivedAt is stamped by the Collector (ingest time), so assert it is a
    // valid ISO timestamp rather than a fixed value, then check the rest of the
    // envelope round-tripped exactly. projectId is the authenticated project.
    const { receivedAt, ...rest } = event as RawEvent;
    expect(Number.isNaN(Date.parse(receivedAt))).toBe(false);
    expect(rest).toEqual({
      eventId,
      projectId,
      schemaVersion: RAW_EVENT_SCHEMA_VERSION,
      type: sent.type,
      occurredAt: sent.occurredAt,
      payload: sent.payload,
      sessionId: sent.sessionId,
      actorId: sent.actorId,
      source: sent.source,
    });
  });

  it('rejects an unauthenticated request (no API key) with 401', async () => {
    await request(collector.getHttpServer() as Server)
      .post('/collect')
      .send({ type: 'level_complete', payload: { level: 1 } })
      .expect(401);
  });
});

/** Boot the Project/Schema hybrid app (HTTP admin + gRPC) used by the Collector. */
async function bootProjectSchema(): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({ imports: [ProjectSchemaAppModule] }).compile();
  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(
    new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
  );
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: PROJECT_SCHEMA_PROTO_PACKAGE,
      protoPath: PROJECT_SCHEMA_PROTO_PATH,
      url: process.env.GRPC_URL,
    },
  });
  await app.init(); // runs `prisma migrate deploy` + connects Prisma
  await app.startAllMicroservices(); // bind the gRPC server
  return app;
}

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
