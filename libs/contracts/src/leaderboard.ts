import { z } from 'zod';

/**
 * Live-leaderboard contract (KAN-34, ADR-0015 §2). The Aggregator writes a
 * per-project **Redis sorted set (ZSET)** of player scores; the Query API reads
 * top-N and a player's rank/score from it. These helpers are the single source
 * of truth for the **key scheme**, imported by both the writer and the reader so
 * the two can never drift — the same discipline `time-window.ts` applies to the
 * Cassandra bucket keys.
 *
 * Score semantics are **best-score** (`ZADD … GT`): a member's score only ever
 * moves up, which makes the update naturally idempotent and replay-safe, so no
 * dedup gate is needed for this view (ADR-0016 §1).
 */

/** Redis key prefix for leaderboard sorted sets. */
const LEADERBOARD_KEY_PREFIX = 'lb';

/** The all-time period token — a board that never expires. */
export const LEADERBOARD_ALLTIME_PERIOD = 'alltime';

/**
 * Build the Redis key for a project's leaderboard at a given period:
 * `lb:{projectId}:{period}`, where `period` is {@link LEADERBOARD_ALLTIME_PERIOD}
 * or a UTC calendar day `YYYY-MM-DD` ({@link dailyLeaderboardPeriod}).
 */
export function leaderboardKey(projectId: string, period: string): string {
  return `${LEADERBOARD_KEY_PREFIX}:${projectId}:${period}`;
}

/**
 * The UTC calendar-day period token (`YYYY-MM-DD`) for a daily board, derived
 * from the event's **`occurredAt`** (event time) so a late/out-of-order event
 * lands on the day it happened — consistent with the event-time windowing in
 * ADR-0015 §3. Falls back to the current day for missing/unparseable input so an
 * event is never silently dropped.
 */
export function dailyLeaderboardPeriod(iso: string | undefined): string {
  const date = iso ? new Date(iso) : new Date();
  const valid = Number.isNaN(date.getTime()) ? new Date() : date;
  return valid.toISOString().slice(0, 10); // 'YYYY-MM-DD'
}

/**
 * Validates a leaderboard `period` request param: the all-time token or a UTC
 * calendar day `YYYY-MM-DD`. The Query API uses this to validate `?period=`.
 */
export const leaderboardPeriodSchema = z
  .string()
  .regex(
    new RegExp(`^(${LEADERBOARD_ALLTIME_PERIOD}|\\d{4}-\\d{2}-\\d{2})$`),
    'period must be "alltime" or a UTC date (YYYY-MM-DD)',
  );

/** One ranked entry on a leaderboard. `rank` is **1-based** (1 = top). */
export const leaderboardEntrySchema = z.object({
  playerId: z.string(),
  score: z.number(),
  rank: z.number().int().positive(),
});
export type LeaderboardEntry = z.infer<typeof leaderboardEntrySchema>;

/** `GET /leaderboard` — top-N for a `(projectId, period)`. */
export const topNResponseSchema = z.object({
  projectId: z.string(),
  period: z.string(),
  entries: z.array(leaderboardEntrySchema),
});
export type TopNResponse = z.infer<typeof topNResponseSchema>;

/** `GET /leaderboard/rank` — a single player's standing on a board. */
export const playerRankResponseSchema = z.object({
  projectId: z.string(),
  period: z.string(),
  playerId: z.string(),
  rank: z.number().int().positive(),
  score: z.number(),
});
export type PlayerRankResponse = z.infer<typeof playerRankResponseSchema>;
