import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

// Integration test for the Project/Schema service (KAN-28): drives the real HTTP
// API against a real Postgres (Testcontainers), with migrations applied by the
// service on bootstrap. Covers the acceptance criteria end-to-end —
// create-project → issue-key → verify-key → register-schema → fetch/list →
// revoke-key → verify-fails. Set SKIP_INTEGRATION=1 to skip where Docker is
// unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')(
  'Project/Schema service (integration)',
  () => {
    let container: StartedPostgreSqlContainer;
    let app: INestApplication;
    let http: ReturnType<typeof request>;

    beforeAll(async () => {
      container = await new PostgreSqlContainer('postgres:16-alpine').start();
      // DatabaseService (PrismaClient) and AppConfigModule read these at boot.
      process.env.DATABASE_URL = container.getConnectionUri();
      // app.init() never listens, so PORT is only here to satisfy config validation.
      process.env.PORT = '3004';

      // Imported lazily so the env vars above are set before the modules load.
      const { AppModule } = await import('../src/app.module');
      const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
      app = moduleRef.createNestApplication();
      await app.init(); // triggers DatabaseService.onApplicationBootstrap → migrate deploy + connect
      http = request(app.getHttpServer());
    });

    afterAll(async () => {
      await app?.close();
      await container?.stop();
    });

    it('round-trips a project, key, schema, and revocation', async () => {
      // 1. create a project
      const created = await http.post('/projects').send({ name: 'Galaxy Raiders' }).expect(201);
      const projectId = created.body.id as string;
      expect(projectId).toMatch(/^[0-9a-f-]{36}$/);
      expect(created.body.name).toBe('Galaxy Raiders');

      // 2. issue an API key — plaintext returned once, hash never exposed
      const issued = await http.post(`/projects/${projectId}/keys`).expect(201);
      const key = issued.body.key as string;
      const keyId = issued.body.id as string;
      expect(key.startsWith('cas_')).toBe(true);
      expect(issued.body).not.toHaveProperty('hash');
      expect(issued.body).not.toHaveProperty('secret');
      expect(issued.body.revokedAt).toBeNull();

      // 3. verify-key passes
      await http
        .post('/api-keys/verify')
        .send({ key })
        .expect(200)
        .expect({ valid: true, projectId });

      // a wrong key is rejected (data, not error)
      await http
        .post('/api-keys/verify')
        .send({ key: 'cas_deadbeef.totally-wrong-secret' })
        .expect(200)
        .expect({ valid: false });

      // 4. register an event schema and fetch it by (projectId, eventType)
      const jsonSchema = {
        type: 'object',
        properties: { level: { type: 'integer' } },
        required: ['level'],
      };
      await http
        .post(`/projects/${projectId}/schemas`)
        .send({ eventType: 'level_complete', jsonSchema })
        .expect(201);

      const fetched = await http.get(`/projects/${projectId}/schemas/level_complete`).expect(200);
      expect(fetched.body.eventType).toBe('level_complete');
      expect(fetched.body.jsonSchema).toEqual(jsonSchema);

      // list returns the one schema
      const listed = await http.get(`/projects/${projectId}/schemas`).expect(200);
      expect(listed.body).toHaveLength(1);
      expect(listed.body[0].eventType).toBe('level_complete');

      // re-registering the same type upserts (no duplicate)
      await http
        .post(`/projects/${projectId}/schemas`)
        .send({ eventType: 'level_complete', jsonSchema: { type: 'object' } })
        .expect(201);
      const relisted = await http.get(`/projects/${projectId}/schemas`).expect(200);
      expect(relisted.body).toHaveLength(1);

      // 5. revoke the key → verify now fails
      const revoked = await http.post(`/projects/${projectId}/keys/${keyId}/revoke`).expect(200);
      expect(revoked.body.revokedAt).not.toBeNull();

      await http.post('/api-keys/verify').send({ key }).expect(200).expect({ valid: false });
    });

    it('404s when issuing a key for an unknown project', async () => {
      await http.post('/projects/00000000-0000-0000-0000-000000000000/keys').expect(404);
    });

    it('404s when fetching a schema that was never registered', async () => {
      const created = await http.post('/projects').send({ name: 'Empty' }).expect(201);
      await http.get(`/projects/${created.body.id}/schemas/nope`).expect(404);
    });

    it("404s when revoking a key through a project that doesn't own it", async () => {
      const p1 = await http.post('/projects').send({ name: 'P1' }).expect(201);
      const p2 = await http.post('/projects').send({ name: 'P2' }).expect(201);
      const key = await http.post(`/projects/${p1.body.id}/keys`).expect(201);
      // p1's key id revoked via p2 — the lookup is project-scoped, so this is a 404.
      await http.post(`/projects/${p2.body.id}/keys/${key.body.id}/revoke`).expect(404);
    });

    it('400s (not 500) on a non-UUID projectId', async () => {
      await http.get('/projects/not-a-uuid/schemas').expect(400);
    });
  },
);
