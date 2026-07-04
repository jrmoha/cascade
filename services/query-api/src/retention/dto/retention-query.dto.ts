import { Type } from 'class-transformer';
import { IsInt, IsNotEmpty, IsOptional, IsString, Matches, Max, Min } from 'class-validator';
import { RETENTION_DEFAULT_MAX_OFFSET, RETENTION_MAX_OFFSET } from '@cascade/contracts';

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Query string for `GET /retention` (KAN-35). Returns the cohort matrix for
 * cohorts (actors first seen on a UTC day) within `[from, to]`, up to `maxOffset`
 * days after each cohort day.
 *
 * `from`/`to` are UTC calendar days (`YYYY-MM-DD`) — the same granularity the
 * retention read model is bucketed at. The range width and `from <= to` are
 * enforced in the controller, which can return a meaningful `400`.
 */
export class RetentionQueryDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @Matches(DATE_PATTERN, { message: 'from must be a UTC date (YYYY-MM-DD)' })
  from!: string;

  @Matches(DATE_PATTERN, { message: 'to must be a UTC date (YYYY-MM-DD)' })
  to!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(RETENTION_MAX_OFFSET)
  maxOffset: number = RETENTION_DEFAULT_MAX_OFFSET;
}
