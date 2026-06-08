import { IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';
import { LEADERBOARD_ALLTIME_PERIOD } from '@cascade/contracts';

/** `period` must be the all-time token or a UTC calendar day `YYYY-MM-DD`. */
const PERIOD_PATTERN = new RegExp(`^(${LEADERBOARD_ALLTIME_PERIOD}|\\d{4}-\\d{2}-\\d{2})$`);

/**
 * Query string for `GET /leaderboard/rank` (KAN-34): one player's rank + score on
 * a `(projectId, period)` board. Returns `404` when the player isn't on the board.
 */
export class PlayerRankQueryDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @IsString()
  @IsNotEmpty()
  playerId!: string;

  /** Board scope: `alltime` (default) or a UTC day `YYYY-MM-DD`. */
  @IsOptional()
  @IsString()
  @Matches(PERIOD_PATTERN, { message: 'period must be "alltime" or a UTC date (YYYY-MM-DD)' })
  period: string = LEADERBOARD_ALLTIME_PERIOD;
}
