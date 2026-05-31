import { afterEach, describe, expect, it, vi } from 'vitest';
import type { CassandraService } from '../src/cassandra/cassandra.service';
import { RawEventReadRepository } from '../src/query/raw-event.read-repository';

// A minimal stand-in for a cassandra-driver Row: rows expose `get(column)`.
function row(fields: Record<string, unknown>) {
  return { get: (col: string) => fields[col] };
}

function uuid(value: string) {
  return { toString: () => value };
}

describe('RawEventReadRepository', () => {
  afterEach(() => vi.useRealTimers());

  it('fans out one single-partition query per hourly window', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-05-30T15:30:00.000Z'));

    const execute = vi.fn().mockResolvedValue({ rows: [] });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    await repo.readRecent('game-1', 3);

    expect(execute).toHaveBeenCalledTimes(3);
    const windowsQueried = execute.mock.calls.map((call) => call[1][1]);
    expect(windowsQueried).toEqual(['2026-05-30T15', '2026-05-30T14', '2026-05-30T13']);
    // Every call is bound to the project and uses a prepared statement.
    for (const call of execute.mock.calls) {
      expect(call[1][0]).toBe('game-1');
      expect(call[2]).toEqual({ prepare: true });
    }
  });

  it('maps rows to the RawEvent envelope and returns them newest-first', async () => {
    const execute = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [
          row({
            event_id: uuid('11111111-1111-4111-8111-111111111111'),
            project_id: 'game-1',
            type: 'level_complete',
            occurred_at: new Date('2026-05-30T15:10:00.000Z'),
            received_at: new Date('2026-05-30T15:10:00.500Z'),
            payload: JSON.stringify({ level: 3 }),
            session_id: 'sess-9',
            actor_id: 'player-42',
            source: 'unity-sdk@1.4.0',
          }),
        ],
      })
      .mockResolvedValueOnce({
        rows: [
          row({
            event_id: uuid('22222222-2222-4222-8222-222222222222'),
            project_id: 'game-1',
            type: 'level_start',
            occurred_at: new Date('2026-05-30T14:50:00.000Z'),
            received_at: new Date('2026-05-30T14:50:00.500Z'),
            payload: JSON.stringify({ level: 4 }),
            session_id: null,
            actor_id: null,
            source: null,
          }),
        ],
      });

    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);
    const events = await repo.readRecent('game-1', 2);

    expect(events).toEqual([
      {
        eventId: '11111111-1111-4111-8111-111111111111',
        projectId: 'game-1',
        type: 'level_complete',
        occurredAt: '2026-05-30T15:10:00.000Z',
        receivedAt: '2026-05-30T15:10:00.500Z',
        payload: { level: 3 },
        sessionId: 'sess-9',
        actorId: 'player-42',
        source: 'unity-sdk@1.4.0',
      },
      {
        // Optional columns are null here → absent from the read-back envelope.
        eventId: '22222222-2222-4222-8222-222222222222',
        projectId: 'game-1',
        type: 'level_start',
        occurredAt: '2026-05-30T14:50:00.000Z',
        receivedAt: '2026-05-30T14:50:00.500Z',
        payload: { level: 4 },
      },
    ]);
  });

  it('returns an empty list when no partitions hold rows', async () => {
    const execute = vi.fn().mockResolvedValue({ rows: [] });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    await expect(repo.readRecent('unknown', 1)).resolves.toEqual([]);
  });
});
