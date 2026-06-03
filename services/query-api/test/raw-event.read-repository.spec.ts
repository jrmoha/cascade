import { describe, expect, it, vi } from 'vitest';
import { RAW_EVENT_SCHEMA_VERSION } from '@cascade/contracts';
import type { CassandraService } from '../src/cassandra/cassandra.service';
import { InvalidCursorError, RawEventReadRepository } from '../src/query/raw-event.read-repository';

// A minimal stand-in for a cassandra-driver Row: rows expose `get(column)`.
function row(fields: Record<string, unknown>) {
  return { get: (col: string) => fields[col] };
}

function uuid(value: string) {
  return { toString: () => value };
}

function decodeCursor(cursor: string): unknown {
  return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
}

// A 3-bucket window: 13:00 → 15:30 covers hourly buckets 15, 14, 13 (newest-first).
const FROM = '2026-05-30T13:00:00.000Z';
const TO = '2026-05-30T15:30:00.000Z';
const BUCKETS = ['2026-05-30T15', '2026-05-30T14', '2026-05-30T13'];

describe('RawEventReadRepository.readWindow', () => {
  it('reads one bounded single-partition query per bucket in the window, newest-first', async () => {
    const execute = vi.fn().mockResolvedValue({ rows: [], pageState: undefined });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    const { events, nextCursor } = await repo.readWindow({
      projectId: 'game-1',
      from: FROM,
      to: TO,
      limit: 100,
    });

    expect(events).toEqual([]);
    expect(nextCursor).toBeUndefined();

    // One query per bucket, newest-first, each bound to the partition key plus
    // the occurred_at range — never a cross-partition scan.
    expect(execute).toHaveBeenCalledTimes(3);
    expect(execute.mock.calls.map((c) => c[1][1])).toEqual(BUCKETS);
    for (const call of execute.mock.calls) {
      const [projectId, , fromDate, toDate] = call[1];
      expect(projectId).toBe('game-1');
      expect(fromDate).toBeInstanceOf(Date);
      expect((fromDate as Date).toISOString()).toBe(FROM);
      expect((toDate as Date).toISOString()).toBe(TO);
      expect(call[2].prepare).toBe(true);
    }
    // No ALLOW FILTERING in the statement.
    expect(execute.mock.calls[0][0]).not.toMatch(/ALLOW FILTERING/i);
  });

  it('maps rows to the RawEvent envelope and concatenates buckets newest-first', async () => {
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
        pageState: undefined,
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
        pageState: undefined,
      })
      .mockResolvedValueOnce({ rows: [], pageState: undefined });

    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);
    const { events } = await repo.readWindow({
      projectId: 'game-1',
      from: FROM,
      to: TO,
      limit: 100,
    });

    expect(events).toEqual([
      {
        eventId: '11111111-1111-4111-8111-111111111111',
        projectId: 'game-1',
        schemaVersion: RAW_EVENT_SCHEMA_VERSION,
        type: 'level_complete',
        occurredAt: '2026-05-30T15:10:00.000Z',
        receivedAt: '2026-05-30T15:10:00.500Z',
        payload: { level: 3 },
        sessionId: 'sess-9',
        actorId: 'player-42',
        source: 'unity-sdk@1.4.0',
      },
      {
        // Optional columns null here → absent from the read-back envelope.
        eventId: '22222222-2222-4222-8222-222222222222',
        projectId: 'game-1',
        schemaVersion: RAW_EVENT_SCHEMA_VERSION,
        type: 'level_start',
        occurredAt: '2026-05-30T14:50:00.000Z',
        receivedAt: '2026-05-30T14:50:00.500Z',
        payload: { level: 4 },
      },
    ]);
  });

  it('returns a cursor pinned to the current bucket when a partition has more rows', async () => {
    const r = row({
      event_id: uuid('11111111-1111-4111-8111-111111111111'),
      project_id: 'game-1',
      type: 't',
      occurred_at: new Date('2026-05-30T15:10:00.000Z'),
      received_at: new Date('2026-05-30T15:10:00.000Z'),
      payload: '{}',
    });
    const execute = vi.fn().mockResolvedValueOnce({ rows: [r, r], pageState: 'PS1' });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    const { events, nextCursor } = await repo.readWindow({
      projectId: 'game-1',
      from: FROM,
      to: TO,
      limit: 2,
    });

    expect(events).toHaveLength(2);
    // Only the first bucket was queried — paging stopped as soon as the page filled.
    expect(execute).toHaveBeenCalledTimes(1);
    expect(execute.mock.calls[0][2]).toEqual({ prepare: true, fetchSize: 2, pageState: undefined });
    expect(nextCursor).toBeDefined();
    expect(decodeCursor(nextCursor as string)).toEqual({ b: '2026-05-30T15', p: 'PS1' });
  });

  it('resumes a within-bucket cursor by replaying its driver paging-state', async () => {
    const cursor = Buffer.from(JSON.stringify({ b: '2026-05-30T15', p: 'PS1' })).toString(
      'base64url',
    );
    const execute = vi.fn().mockResolvedValue({ rows: [], pageState: undefined });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    await repo.readWindow({ projectId: 'game-1', from: FROM, to: TO, limit: 2, cursor });

    // First query resumes bucket 15 with the carried paging-state…
    expect(execute.mock.calls[0][1][1]).toBe('2026-05-30T15');
    expect(execute.mock.calls[0][2].pageState).toBe('PS1');
    // …then continues into the older buckets with a fresh state.
    expect(execute.mock.calls[1][1][1]).toBe('2026-05-30T14');
    expect(execute.mock.calls[1][2].pageState).toBeUndefined();
  });

  it('fills a page across bucket boundaries, requesting only the remaining rows', async () => {
    const mk = (bucketHour: string) =>
      row({
        event_id: uuid('11111111-1111-4111-8111-111111111111'),
        project_id: 'game-1',
        type: 't',
        occurred_at: new Date(`2026-05-30T${bucketHour}:10:00.000Z`),
        received_at: new Date(`2026-05-30T${bucketHour}:10:00.000Z`),
        payload: '{}',
      });
    const execute = vi
      .fn()
      // bucket 15: two rows, partition exhausted (no pageState).
      .mockResolvedValueOnce({ rows: [mk('15'), mk('15')], pageState: undefined })
      // bucket 14: one more row, partition has further rows (pageState set).
      .mockResolvedValueOnce({ rows: [mk('14')], pageState: 'PS2' });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    const { events, nextCursor } = await repo.readWindow({
      projectId: 'game-1',
      from: FROM,
      to: TO,
      limit: 3,
    });

    expect(events).toHaveLength(3);
    expect(execute).toHaveBeenCalledTimes(2);
    // The second query only asked for the one remaining row.
    expect(execute.mock.calls[1][2].fetchSize).toBe(1);
    expect(decodeCursor(nextCursor as string)).toEqual({ b: '2026-05-30T14', p: 'PS2' });
  });

  it('points the cursor at the next bucket when a partition is exhausted exactly at the limit', async () => {
    const r = row({
      event_id: uuid('11111111-1111-4111-8111-111111111111'),
      project_id: 'game-1',
      type: 't',
      occurred_at: new Date('2026-05-30T15:10:00.000Z'),
      received_at: new Date('2026-05-30T15:10:00.000Z'),
      payload: '{}',
    });
    // bucket 15 returns exactly `limit` rows and is exhausted (no pageState).
    const execute = vi.fn().mockResolvedValueOnce({ rows: [r, r], pageState: undefined });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    const { nextCursor } = await repo.readWindow({
      projectId: 'game-1',
      from: FROM,
      to: TO,
      limit: 2,
    });

    expect(execute).toHaveBeenCalledTimes(1);
    // Resume at the start of the next-older bucket (no paging-state).
    expect(decodeCursor(nextCursor as string)).toEqual({ b: '2026-05-30T14' });
  });

  it('omits the cursor once the whole window is read', async () => {
    const execute = vi.fn().mockResolvedValue({ rows: [], pageState: undefined });
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    const { events, nextCursor } = await repo.readWindow({
      projectId: 'unknown',
      from: '2026-05-30T15:00:00.000Z',
      to: '2026-05-30T15:30:00.000Z',
      limit: 100,
    });

    expect(events).toEqual([]);
    expect(nextCursor).toBeUndefined();
  });

  it('rejects a malformed cursor without querying', async () => {
    const execute = vi.fn();
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);

    await expect(
      repo.readWindow({ projectId: 'game-1', from: FROM, to: TO, limit: 100, cursor: 'not-valid' }),
    ).rejects.toBeInstanceOf(InvalidCursorError);
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects a cursor whose bucket falls outside the requested window', async () => {
    const execute = vi.fn();
    const repo = new RawEventReadRepository({ execute } as unknown as CassandraService);
    const cursor = Buffer.from(JSON.stringify({ b: '2026-05-30T20' })).toString('base64url');

    await expect(
      repo.readWindow({ projectId: 'game-1', from: FROM, to: TO, limit: 100, cursor }),
    ).rejects.toBeInstanceOf(InvalidCursorError);
    expect(execute).not.toHaveBeenCalled();
  });
});
