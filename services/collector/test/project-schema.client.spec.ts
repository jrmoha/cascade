import { ServiceUnavailableException, UnprocessableEntityException } from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import type Redis from 'ioredis';
import { of, throwError } from 'rxjs';
import { describe, expect, it, vi } from 'vitest';
import type { CollectorConfig } from '../src/config/env.schema';
import { ProjectSchemaClient } from '../src/ingest/project-schema.client';

const SCHEMA = {
  type: 'object',
  properties: { level: { type: 'integer' } },
  required: ['level'],
  additionalProperties: true,
};

/** In-memory Redis double — only get/set with EX are used by the client. */
function fakeRedis(): Redis & { store: Map<string, string> } {
  const store = new Map<string, string>();
  return {
    store,
    get: vi.fn(async (key: string) => store.get(key) ?? null),
    set: vi.fn(async (key: string, value: string) => {
      store.set(key, value);
      return 'OK';
    }),
  } as unknown as Redis & { store: Map<string, string> };
}

function makeClient(grpcImpl: { VerifyKey?: unknown; GetEventSchema?: unknown }) {
  const redis = fakeRedis();
  const grpc = {
    VerifyKey: vi.fn(grpcImpl.VerifyKey as never),
    GetEventSchema: vi.fn(grpcImpl.GetEventSchema as never),
  };
  const clientGrpc = { getService: () => grpc } as unknown as ClientGrpc;
  const config = { PROJECT_SCHEMA_CACHE_TTL_SECONDS: 30 } as CollectorConfig;
  const client = new ProjectSchemaClient(clientGrpc, redis, config);
  client.onModuleInit();
  return { client, grpc, redis };
}

describe('ProjectSchemaClient.resolveProjectId', () => {
  it('verifies a cold key over gRPC, then serves the second call from cache', async () => {
    const { client, grpc } = makeClient({
      VerifyKey: () => of({ valid: true, projectId: 'proj-1' }),
    });

    expect(await client.resolveProjectId('cas_a.secret')).toBe('proj-1');
    expect(await client.resolveProjectId('cas_a.secret')).toBe('proj-1');
    expect(grpc.VerifyKey).toHaveBeenCalledTimes(1); // second call hit Redis
  });

  it('returns null for an invalid key and negatively caches it', async () => {
    const { client, grpc } = makeClient({ VerifyKey: () => of({ valid: false }) });

    expect(await client.resolveProjectId('cas_bad.key')).toBeNull();
    expect(await client.resolveProjectId('cas_bad.key')).toBeNull();
    expect(grpc.VerifyKey).toHaveBeenCalledTimes(1); // negative cached
  });

  it('fails closed with 503 when Project/Schema is unreachable on a cold cache', async () => {
    const { client } = makeClient({
      VerifyKey: () => throwError(() => ({ code: status.UNAVAILABLE, message: 'down' })),
    });

    await expect(client.resolveProjectId('cas_a.secret')).rejects.toBeInstanceOf(
      ServiceUnavailableException,
    );
  });
});

describe('ProjectSchemaClient.validatePayload', () => {
  const eventSchema = (updatedAt = 't1') => ({
    id: 's1',
    projectId: 'proj-1',
    eventType: 'level_complete',
    jsonSchema: JSON.stringify(SCHEMA),
    createdAt: 't0',
    updatedAt,
  });

  it('accepts a valid payload and caches the schema (one gRPC fetch)', async () => {
    const { client, grpc } = makeClient({ GetEventSchema: () => of(eventSchema()) });

    await expect(
      client.validatePayload('proj-1', 'level_complete', { level: 5 }),
    ).resolves.toBeUndefined();
    await expect(
      client.validatePayload('proj-1', 'level_complete', { level: 6 }),
    ).resolves.toBeUndefined();
    expect(grpc.GetEventSchema).toHaveBeenCalledTimes(1);
  });

  it('rejects a payload that violates the schema with a structured 400', async () => {
    const { client } = makeClient({ GetEventSchema: () => of(eventSchema()) });

    await expect(
      client.validatePayload('proj-1', 'level_complete', { level: 'not-an-int' }),
    ).rejects.toMatchObject({
      // BadRequestException with the shared structured body
      response: {
        statusCode: 400,
        message: 'Event validation failed',
        errors: expect.arrayContaining([expect.objectContaining({ field: 'level' })]),
      },
    });
  });

  it('maps an unregistered type (gRPC NOT_FOUND) to 422 and negatively caches it', async () => {
    const { client, grpc } = makeClient({
      GetEventSchema: () => throwError(() => ({ code: status.NOT_FOUND })),
    });

    await expect(client.validatePayload('proj-1', 'mystery', { level: 1 })).rejects.toBeInstanceOf(
      UnprocessableEntityException,
    );
    await expect(client.validatePayload('proj-1', 'mystery', { level: 1 })).rejects.toBeInstanceOf(
      UnprocessableEntityException,
    );
    expect(grpc.GetEventSchema).toHaveBeenCalledTimes(1); // negative cached
  });

  it('fails closed with 503 when the schema is uncached and Project/Schema is down', async () => {
    const { client } = makeClient({
      GetEventSchema: () => throwError(() => ({ code: status.UNAVAILABLE })),
    });

    await expect(client.validatePayload('proj-1', 'level_complete', {})).rejects.toBeInstanceOf(
      ServiceUnavailableException,
    );
  });
});
