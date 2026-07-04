import { IsISO8601, IsIn, IsNotEmpty, IsOptional, IsString } from 'class-validator';
import type { CountsGranularity } from '@cascade/contracts';

/**
 * Query string for `GET /counts` (KAN-36). Returns a time-series of per-bucket
 * event counts for a project over an inclusive event-time window `[from, to]`,
 * served from the Aggregator's `event_counts_by_minute` / `event_counts_by_hour`
 * counter tables — never `raw_events` (ADR-0015 / ADR-0018).
 *
 * `granularity` selects the bucket size (and thus which counter table is read);
 * it defaults to `hour`. `type` optionally narrows the read to a single event
 * type. `from`/`to` are ISO-8601 instants bounding `occurredAt`.
 */
export class CountsQueryDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @IsISO8601()
  from!: string;

  @IsISO8601()
  to!: string;

  @IsOptional()
  @IsIn(['minute', 'hour'])
  granularity: CountsGranularity = 'hour';

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  type?: string;
}
