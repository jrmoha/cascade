import { describe, expect, it, vi } from 'vitest';
import { types } from 'cassandra-driver';
import { BucketSpanError, CountsRepository } from '../src/counts/counts.repository';
import type { CassandraService } from '../src/cassandra/cassandra.service';

/** Build a fake Cassandra row exposing `.get(column)` like the driver's Row. */
function row(values: Record<string, unknown>): types.Row {
  return { get: (col: string) => values[col] } as unknown as types.Row;
}

/** A `counter` column value as the driver surfaces it: a `Long` that
 * stringifies to its decimal value (what `toCount` relies on). */
function long(n: number): types.Long {
  return { toString: () => String(n) } as unknown as types.Long;
}

/** A CassandraService stub whose `execute` returns queued result sets in order. */
function stubCassandra(pages: types.Row[][]): {
  cassandra: CassandraService;
  execute: ReturnType<typeof vi.fn>;
} {
  const execute = vi.fn();
  for (const rows of pages) execute.mockResolvedValueOnce({ rows });
  return { cassandra: { execute } as unknown as CassandraService, execute };
}

describe('CountsRepository', () => {
  it('walks hourly buckets newest-first and reads each as a single partition', async () => {
    // Window [10:30, 11:30] → hourly buckets 11 and 10 (newest-first).
    const { cassandra, execute } = stubCassandra([
      [row({ event_type: 'login', count: long(5) })],
      [row({ event_type: 'login', count: long(3) })],
    ]);
    const repo = new CountsRepository(cassandra);

    const result = await repo.read({
      projectId: 'game-1',
      from: '2026-05-30T10:30:00.000Z',
      to: '2026-05-30T11:30:00.000Z',
      granularity: 'hour',
    });

    expect(execute).toHaveBeenCalledTimes(2);
    // First call reads the newest bucket, prepared, single-partition (no ALLOW FILTERING).
    const [sql, params, options] = execute.mock.calls[0];
    expect(sql).toContain('event_counts_by_hour');
    expect(sql).not.toMatch(/ALLOW FILTERING/i);
    expect(params).toEqual(['game-1', '2026-05-30T11']);
    expect(options).toEqual({ prepare: true });
    expect(execute.mock.calls[1][1]).toEqual(['game-1', '2026-05-30T10']);

    expect(result).toEqual([
      { bucket: '2026-05-30T11', eventType: 'login', count: 5 },
      { bucket: '2026-05-30T10', eventType: 'login', count: 3 },
    ]);
  });

  it('reads the minute table and narrows by event type when `type` is given', async () => {
    const { cassandra, execute } = stubCassandra([[row({ event_type: 'score', count: long(2) })]]);
    const repo = new CountsRepository(cassandra);

    const result = await repo.read({
      projectId: 'game-1',
      from: '2026-05-30T10:00:00.000Z',
      to: '2026-05-30T10:00:30.000Z',
      granularity: 'minute',
      type: 'score',
    });

    const [sql, params] = execute.mock.calls[0];
    expect(sql).toContain('event_counts_by_minute');
    expect(sql).toContain('event_type = ?');
    expect(params).toEqual(['game-1', '2026-05-30T10:00', 'score']);
    expect(result).toEqual([{ bucket: '2026-05-30T10:00', eventType: 'score', count: 2 }]);
  });

  it('rejects a window that exceeds the minute-bucket cap (bounded fan-out)', async () => {
    const { cassandra, execute } = stubCassandra([]);
    const repo = new CountsRepository(cassandra);

    // > 1440 minutes (24h) at minute granularity.
    await expect(
      repo.read({
        projectId: 'game-1',
        from: '2026-05-30T00:00:00.000Z',
        to: '2026-06-01T00:00:00.000Z',
        granularity: 'minute',
      }),
    ).rejects.toBeInstanceOf(BucketSpanError);
    // Guardrail trips before any Cassandra read.
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects an absurdly wide window in O(1) without materializing the buckets', async () => {
    const { cassandra, execute } = stubCassandra([]);
    const repo = new CountsRepository(cassandra);

    // A ~26-year minute window would be ~13.7M buckets. The span is checked
    // arithmetically, so this returns immediately rather than allocating them.
    const started = Date.now();
    await expect(
      repo.read({
        projectId: 'game-1',
        from: '2000-01-01T00:00:00.000Z',
        to: '2026-01-01T00:00:00.000Z',
        granularity: 'minute',
      }),
    ).rejects.toBeInstanceOf(BucketSpanError);
    expect(execute).not.toHaveBeenCalled();
    // Should be effectively instant (no multi-million-element array build).
    expect(Date.now() - started).toBeLessThan(200);
  });
});
