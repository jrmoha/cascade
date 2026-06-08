import { describe, expect, it, vi } from 'vitest';
import { NotFoundException } from '@nestjs/common';
import type Redis from 'ioredis';
import { LeaderboardService } from '../src/leaderboard/leaderboard.service';

describe('LeaderboardService', () => {
  it('maps ZREVRANGE WITHSCORES to 1-based ranked entries', async () => {
    const zrevrange = vi.fn().mockResolvedValue(['p2', '350', 'p3', '200', 'p1', '100']);
    const svc = new LeaderboardService({ zrevrange } as unknown as Redis);

    const res = await svc.topN({ projectId: 'arena', period: 'alltime', limit: 10 });

    // ZREVRANGE key 0 limit-1 WITHSCORES — limit-1 because the range is inclusive.
    expect(zrevrange).toHaveBeenCalledWith('lb:arena:alltime', 0, 9, 'WITHSCORES');
    expect(res).toEqual({
      projectId: 'arena',
      period: 'alltime',
      entries: [
        { playerId: 'p2', score: 350, rank: 1 },
        { playerId: 'p3', score: 200, rank: 2 },
        { playerId: 'p1', score: 100, rank: 3 },
      ],
    });
  });

  it('returns a player rank (1-based) and score from ZREVRANK + ZSCORE', async () => {
    const zrevrank = vi.fn().mockResolvedValue(2); // 0-based
    const zscore = vi.fn().mockResolvedValue('100');
    const svc = new LeaderboardService({ zrevrank, zscore } as unknown as Redis);

    const res = await svc.playerRank({ projectId: 'arena', period: '2026-05-30', playerId: 'p1' });

    expect(zrevrank).toHaveBeenCalledWith('lb:arena:2026-05-30', 'p1');
    expect(res).toEqual({
      projectId: 'arena',
      period: '2026-05-30',
      playerId: 'p1',
      rank: 3, // 0-based 2 → 1-based 3
      score: 100,
    });
  });

  it('throws NotFound when the player is absent from the board', async () => {
    const zrevrank = vi.fn().mockResolvedValue(null);
    const zscore = vi.fn().mockResolvedValue(null);
    const svc = new LeaderboardService({ zrevrank, zscore } as unknown as Redis);

    await expect(
      svc.playerRank({ projectId: 'arena', period: 'alltime', playerId: 'ghost' }),
    ).rejects.toBeInstanceOf(NotFoundException);
  });
});
