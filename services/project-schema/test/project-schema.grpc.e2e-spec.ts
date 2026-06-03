import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  ClientGrpc,
  ClientProxy,
  ClientProxyFactory,
  MicroserviceOptions,
  Transport,
} from '@nestjs/microservices';
import {
  PROJECT_SCHEMA_GRPC_SERVICE,
  PROJECT_SCHEMA_PROTO_PACKAGE,
  PROJECT_SCHEMA_PROTO_PATH,
  projectSchemaProto,
} from '@cascade/contracts';
import { status } from '@grpc/grpc-js';
import request from 'supertest';
import { lastValueFrom, type Observable } from 'rxjs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

/** Typed view of the gRPC client — Nest's client returns Observables. */
interface ProjectSchemaGrpcClient {
  VerifyKey(
    request: projectSchemaProto.VerifyKeyRequest,
  ): Observable<projectSchemaProto.VerifyKeyResponse>;
  GetEventSchema(
    request: projectSchemaProto.GetEventSchemaRequest,
  ): Observable<projectSchemaProto.EventSchema>;
}

const GRPC_ADDR = '127.0.0.1:50251';

// Integration test for the Project/Schema gRPC sync contract (KAN-29): the
// internal call the Collector makes on its ingest hot path. Boots the hybrid app
// (HTTP + gRPC) against a real Postgres, seeds data via the REST admin API, then
// exercises VerifyKey / GetEventSchema over the wire. Set SKIP_INTEGRATION=1 to
// skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Project/Schema gRPC contract (integration)',
  () => {
    let container: StartedPostgreSqlContainer;
    let app: INestApplication;
    let client: ClientProxy & ClientGrpc;
    let grpc: ProjectSchemaGrpcClient;
    let http: ReturnType<typeof request>;

    beforeAll(async () => {
      container = await new PostgreSqlContainer('postgres:16-alpine').start();
      process.env.DATABASE_URL = container.getConnectionUri();
      process.env.PORT = '3004';
      process.env.GRPC_URL = GRPC_ADDR;

      const { AppModule } = await import('../src/app.module');
      const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
      app = moduleRef.createNestApplication();
      app.connectMicroservice<MicroserviceOptions>({
        transport: Transport.GRPC,
        options: {
          package: PROJECT_SCHEMA_PROTO_PACKAGE,
          protoPath: PROJECT_SCHEMA_PROTO_PATH,
          url: GRPC_ADDR,
        },
      });
      await app.init(); // migrate deploy + Prisma connect
      await app.startAllMicroservices(); // bind the gRPC server
      http = request(app.getHttpServer());

      client = ClientProxyFactory.create({
        transport: Transport.GRPC,
        options: {
          package: PROJECT_SCHEMA_PROTO_PACKAGE,
          protoPath: PROJECT_SCHEMA_PROTO_PATH,
          url: GRPC_ADDR,
        },
      }) as ClientProxy & ClientGrpc;
      grpc = client.getService<ProjectSchemaGrpcClient>(PROJECT_SCHEMA_GRPC_SERVICE);
    });

    afterAll(async () => {
      client?.close();
      await app?.close();
      await container?.stop();
    });

    it('VerifyKey returns valid + projectId for an issued key, and rejects bad/revoked keys', async () => {
      const project = await http.post('/projects').send({ name: 'Grpc Raiders' }).expect(201);
      const projectId = project.body.id as string;
      const issued = await http.post(`/projects/${projectId}/keys`).expect(201);
      const key = issued.body.key as string;
      const keyId = issued.body.id as string;

      const ok = await lastValueFrom(grpc.VerifyKey({ key }));
      expect(ok).toEqual({ valid: true, projectId });

      const bad = await lastValueFrom(grpc.VerifyKey({ key: 'cas_deadbeef.totally-wrong-secret' }));
      expect(bad.valid).toBe(false);

      // Revoking via REST is reflected on the gRPC hot path.
      await http.post(`/projects/${projectId}/keys/${keyId}/revoke`).expect(200);
      const afterRevoke = await lastValueFrom(grpc.VerifyKey({ key }));
      expect(afterRevoke.valid).toBe(false);
    });

    it('GetEventSchema returns the registered schema as a JSON string', async () => {
      const project = await http.post('/projects').send({ name: 'Grpc Schemas' }).expect(201);
      const projectId = project.body.id as string;
      const jsonSchema = {
        type: 'object',
        properties: { level: { type: 'integer' } },
        required: ['level'],
      };
      await http
        .post(`/projects/${projectId}/schemas`)
        .send({ eventType: 'level_complete', jsonSchema })
        .expect(201);

      const record = await lastValueFrom(
        grpc.GetEventSchema({ projectId, eventType: 'level_complete' }),
      );
      expect(record.eventType).toBe('level_complete');
      expect(record.projectId).toBe(projectId);
      expect(JSON.parse(record.jsonSchema)).toEqual(jsonSchema);
    });

    it('GetEventSchema maps a missing schema to gRPC NOT_FOUND', async () => {
      const project = await http.post('/projects').send({ name: 'Grpc Empty' }).expect(201);
      const projectId = project.body.id as string;

      await expect(
        lastValueFrom(grpc.GetEventSchema({ projectId, eventType: 'never_registered' })),
      ).rejects.toMatchObject({ code: status.NOT_FOUND });
    });
  },
);
