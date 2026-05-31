import { types } from 'cassandra-driver';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RawEvent } from '@cascade/contracts';
import { CassandraService } from '../src/cassandra/cassandra.service';
import { RawEventRepository } from '../src/processor/raw-event.repository';

describe('RawEventRepository', () => {
  let execute: ReturnType<typeof vi.fn>;
  let repository: RawEventRepository;

  beforeEach(() => {
    execute = vi.fn().mockResolvedValue({});
    const cassandra = { execute } as unknown as CassandraService;
    repository = new RawEventRepository(cassandra);
  });

  const event: RawEvent = {
    eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
    projectId: 'game-1',
    type: 'level_complete',
    occurredAt: '2026-05-30T15:16:50.165Z',
    receivedAt: '2026-05-30T15:16:50.200Z',
    payload: { level: 3 },
  };

  it('inserts with the correct columns, prepared, mapping the partition key', async () => {
    await repository.insert(event);

    expect(execute).toHaveBeenCalledTimes(1);
    const [cql, params, options] = execute.mock.calls[0];

    expect(cql).toContain('INSERT INTO cascade.raw_events');
    expect(options).toEqual({ prepare: true });

    // Column order: project_id, time_bucket, occurred_at, event_id, type,
    // received_at, payload, session_id, actor_id, source.
    const [projectId, timeBucket, occurredAt, eventId, type, receivedAt, payload] = params;
    expect(projectId).toBe('game-1');
    // time_bucket buckets by occurredAt (event time), not receivedAt.
    expect(timeBucket).toBe('2026-05-30T15');
    expect(occurredAt).toBeInstanceOf(Date);
    expect((occurredAt as Date).toISOString()).toBe(event.occurredAt);
    expect(eventId).toBeInstanceOf(types.Uuid);
    expect(eventId.toString()).toBe(event.eventId);
    expect(type).toBe('level_complete');
    expect(receivedAt).toBeInstanceOf(Date);
    expect((receivedAt as Date).toISOString()).toBe(event.receivedAt);
    expect(payload).toBe('{"level":3}');
  });

  it('maps absent optional fields to null', async () => {
    await repository.insert(event);
    const [, , , , , , , sessionId, actorId, source] = execute.mock.calls[0][1];
    expect(sessionId).toBeNull();
    expect(actorId).toBeNull();
    expect(source).toBeNull();
  });

  it('passes through the optional fields when present', async () => {
    await repository.insert({
      ...event,
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
    const [, , , , , , , sessionId, actorId, source] = execute.mock.calls[0][1];
    expect(sessionId).toBe('sess-9');
    expect(actorId).toBe('player-42');
    expect(source).toBe('unity-sdk@1.4.0');
  });

  it('serializes a missing payload as an empty JSON object', async () => {
    await repository.insert({ ...event, payload: undefined as unknown as Record<string, unknown> });
    const params = execute.mock.calls[0][1];
    expect(params[6]).toBe('{}');
  });
});
