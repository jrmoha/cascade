import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import {
  leaderboardKey,
  type LeaderboardEntry,
  type PlayerRankResponse,
  type TopNResponse,
} from '@cascade/contracts';
import type Redis from 'ioredis';
import { REDIS_CLIENT } from '../redis/redis.tokens';
import { PlayerRankQueryDto } from './dto/player-rank-query.dto';
import { TopNQueryDto } from './dto/top-n-query.dto';

/**
 * Serves the live leaderboard read model from Redis sorted sets (ADR-0015 §2,
 * KAN-34) — the Aggregator is the only writer. Ranked reads are O(log n):
 * `ZREVRANGE` for top-N, `ZREVRANK` + `ZSCORE` for a player's standing. Keys are
 * built with the shared `leaderboardKey` helper so writer and reader never drift.
 *
 * Ranks are returned **1-based** (1 = top) for display, converting from Redis's
 * 0-based `ZREVRANK`.
 */
@Injectable()
export class LeaderboardService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /** Top-N players for `(projectId, period)`, highest score first. */
  async topN({ projectId, period, limit }: TopNQueryDto): Promise<TopNResponse> {
    const key = leaderboardKey(projectId, period);
    // ZREVRANGE key 0 limit-1 WITHSCORES → flat [member, score, member, score, …].
    const flat = await this.redis.zrevrange(key, 0, limit - 1, 'WITHSCORES');
    const entries: LeaderboardEntry[] = [];
    for (let i = 0; i < flat.length; i += 2) {
      entries.push({ playerId: flat[i], score: Number(flat[i + 1]), rank: i / 2 + 1 });
    }
    return { projectId, period, entries };
  }

  /** A single player's rank + score on `(projectId, period)`; 404 if absent. */
  async playerRank({
    projectId,
    period,
    playerId,
  }: PlayerRankQueryDto): Promise<PlayerRankResponse> {
    const key = leaderboardKey(projectId, period);
    const [rank, score] = await Promise.all([
      this.redis.zrevrank(key, playerId),
      this.redis.zscore(key, playerId),
    ]);
    if (rank === null || score === null) {
      throw new NotFoundException(
        `Player "${playerId}" is not on leaderboard ${projectId}/${period}`,
      );
    }
    return { projectId, period, playerId, rank: rank + 1, score: Number(score) };
  }
}
