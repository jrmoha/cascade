import { Type } from 'class-transformer';
import { IsInt, IsNotEmpty, IsOptional, IsString, Matches, Max, Min } from 'class-validator';
import { LEADERBOARD_ALLTIME_PERIOD } from '@cascade/contracts';

/** `period` must be the all-time token or a UTC calendar day `YYYY-MM-DD`. */
const PERIOD_PATTERN = new RegExp(`^(${LEADERBOARD_ALLTIME_PERIOD}|\\d{4}-\\d{2}-\\d{2})$`);

/**
 * Query string for `GET /leaderboard` (KAN-34): top-N for a `(projectId, period)`.
 * Mirrors the Query API's class-validator DTO pattern (the `ValidationPipe`
 * coerces and validates). The key is built from `projectId` + `period`.
 */
export class TopNQueryDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  /** Board scope: `alltime` (default) or a UTC day `YYYY-MM-DD`. */
  @IsOptional()
  @IsString()
  @Matches(PERIOD_PATTERN, { message: 'period must be "alltime" or a UTC date (YYYY-MM-DD)' })
  period: string = LEADERBOARD_ALLTIME_PERIOD;

  /** How many top entries to return. Capped to bound the response. */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(1000)
  limit: number = 100;
}
