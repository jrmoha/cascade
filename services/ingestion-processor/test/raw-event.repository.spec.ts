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
    timestamp: '2026-05-30T15:16:50.165Z',
    payload: { level: 3 },
  };

  it('inserts with the correct columns, prepared, mapping the partition key', async () => {
    await repository.insert(event);

    expect(execute).toHaveBeenCalledTimes(1);
    const [cql, params, options] = execute.mock.calls[0];

    expect(cql).toContain('INSERT INTO cascade.raw_events');
    expect(options).toEqual({ prepare: true });

    const [projectId, timeWindow, eventId, type, eventTime, payload] = params;
    expect(projectId).toBe('game-1');
    expect(timeWindow).toBe('2026-05-30T15');
    expect(eventId).toBeInstanceOf(types.Uuid);
    expect(eventId.toString()).toBe(event.eventId);
    expect(type).toBe('level_complete');
    expect(eventTime).toBeInstanceOf(Date);
    expect((eventTime as Date).toISOString()).toBe(event.timestamp);
    expect(payload).toBe('{"level":3}');
  });

  it('serializes a missing payload as an empty JSON object', async () => {
    await repository.insert({ ...event, payload: undefined as unknown as Record<string, unknown> });
    const params = execute.mock.calls[0][1];
    expect(params[5]).toBe('{}');
  });
});
