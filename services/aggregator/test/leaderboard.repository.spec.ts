import { beforeEach, describe, expect, it, vi } from 'vitest';
import type Redis from 'ioredis';
import type { RawEvent } from '@cascade/contracts';
import { LeaderboardRepository } from '../src/aggregation/leaderboard.repository';
import type { AggregatorConfig } from '../src/config/env.schema';

const DAILY_TTL = 172800;

function event(
  payload: Record<string, unknown>,
  occurredAt = '2026-05-30T15:16:50.000Z',
): RawEvent {
  return {
    eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
    projectId: 'game-1',
    schemaVersion: 1,
    type: 'score',
    occurredAt,
    receivedAt: occurredAt,
    payload,
  };
}

describe('LeaderboardRepository', () => {
  let zadd: ReturnType<typeof vi.fn>;
  let expire: ReturnType<typeof vi.fn>;
  let repo: LeaderboardRepository;

  beforeEach(() => {
    zadd = vi.fn().mockResolvedValue(1);
    expire = vi.fn().mockResolvedValue(1);
    repo = new LeaderboardRepository(
      { zadd, expire } as unknown as Redis,
      {
        AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS: DAILY_TTL,
      } as AggregatorConfig,
    );
  });

  it('writes best-score (ZADD GT) to the all-time and daily boards and sets the daily TTL', async () => {
    await repo.apply(event({ playerId: 'p1', score: 4200 }));

    expect(zadd).toHaveBeenCalledWith('lb:game-1:alltime', 'GT', 4200, 'p1');
    expect(zadd).toHaveBeenCalledWith('lb:game-1:2026-05-30', 'GT', 4200, 'p1');
    // All-time board never expires; only the daily board is given a TTL.
    expect(expire).toHaveBeenCalledTimes(1);
    expect(expire).toHaveBeenCalledWith('lb:game-1:2026-05-30', DAILY_TTL);
  });

  it('buckets the daily board by event time (occurredAt), not wall-clock now', async () => {
    await repo.apply(event({ playerId: 'p1', score: 5 }, '2026-01-02T23:59:00.000Z'));
    expect(zadd).toHaveBeenCalledWith('lb:game-1:2026-01-02', 'GT', 5, 'p1');
  });

  it('ignores events without a non-empty string playerId', async () => {
    await repo.apply(event({ score: 10 }));
    await repo.apply(event({ playerId: 42, score: 10 }));
    await repo.apply(event({ playerId: '', score: 10 }));
    expect(zadd).not.toHaveBeenCalled();
    expect(expire).not.toHaveBeenCalled();
  });

  it('ignores events without a finite numeric score', async () => {
    await repo.apply(event({ playerId: 'p1' }));
    await repo.apply(event({ playerId: 'p1', score: '100' }));
    await repo.apply(event({ playerId: 'p1', score: Number.POSITIVE_INFINITY }));
    await repo.apply(event({ playerId: 'p1', score: Number.NaN }));
    expect(zadd).not.toHaveBeenCalled();
  });
});
