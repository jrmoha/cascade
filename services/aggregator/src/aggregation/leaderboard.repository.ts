import { Inject, Injectable } from '@nestjs/common';
import {
  dailyLeaderboardPeriod,
  LEADERBOARD_ALLTIME_PERIOD,
  leaderboardKey,
  RawEvent,
} from '@cascade/contracts';
import type Redis from 'ioredis';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { REDIS_CLIENT } from '../redis/redis.tokens';

/**
 * Maintains the per-project **live leaderboards** in Redis sorted sets
 * (ADR-0015 §2) — the deliberate "right tool" contrast to the Cassandra counters.
 * A score event updates two boards: an **all-time** board and a **per-UTC-day**
 * board, keyed by `lb:{projectId}:{period}` via the shared `@cascade/contracts`
 * helpers so the writer and the Query API reader agree on the scheme (KAN-34).
 *
 * **Best-score semantics via `ZADD … GT`**: a member's score only ever moves up,
 * so the update is naturally idempotent and replay-safe — re-applying the same
 * event (Kafka at-least-once, or a full replay from offset 0) is a no-op
 * (ADR-0016 §1). This view therefore needs no dedup gate of its own; riding the
 * controller's shared per-`eventId` gate is a harmless superset.
 *
 * A "score event" is any event whose `payload` carries a non-empty string
 * `playerId` and a finite numeric `score`. Events without both simply don't touch
 * a board (no error) — most event types aren't leaderboard-relevant.
 */
@Injectable()
export class LeaderboardRepository {
  private readonly dailyTtlSeconds: number;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(APP_CONFIG) config: AggregatorConfig,
  ) {
    this.dailyTtlSeconds = config.AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS;
  }

  /** Apply one event's score to the all-time and daily boards (no-op if not a score event). */
  async apply(event: RawEvent): Promise<void> {
    const entry = scoreEntry(event);
    if (!entry) return;
    const { playerId, score } = entry;

    // All-time board: never expires. GT = keep the player's best score only.
    const allTimeKey = leaderboardKey(event.projectId, LEADERBOARD_ALLTIME_PERIOD);
    await this.redis.zadd(allTimeKey, 'GT', score, playerId);

    // Daily board, bucketed by event time so a late event lands on its own day;
    // (re)set the TTL on each write so an active board stays alive and an idle one
    // self-expires.
    const dailyKey = leaderboardKey(event.projectId, dailyLeaderboardPeriod(event.occurredAt));
    await this.redis.zadd(dailyKey, 'GT', score, playerId);
    await this.redis.expire(dailyKey, this.dailyTtlSeconds);
  }
}

interface ScoreEntry {
  playerId: string;
  score: number;
}

/** Pull `(playerId, score)` from the payload, or `null` if this isn't a score event. */
function scoreEntry(event: RawEvent): ScoreEntry | null {
  const playerId = event.payload.playerId;
  const score = event.payload.score;
  if (typeof playerId !== 'string' || playerId.length === 0) return null;
  if (typeof score !== 'number' || !Number.isFinite(score)) return null;
  return { playerId, score };
}
