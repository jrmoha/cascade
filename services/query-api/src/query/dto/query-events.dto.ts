import { Type } from 'class-transformer';
import { IsInt, IsISO8601, IsNotEmpty, IsOptional, IsString, Max, Min } from 'class-validator';

/**
 * Query string for `GET /query` (KAN-25). Reads a project's events within an
 * inclusive event-time window `[from, to]`, newest first, with cursor paging.
 *
 * `from`/`to` are ISO-8601 instants bounding `occurredAt` (event time). The
 * window is mapped to the hourly `time_bucket` partitions it covers and read one
 * partition at a time — never a cross-partition scan. The order/width of the
 * window (`from <= to`, span ≤ `MAX_QUERY_BUCKETS`) is enforced in the
 * controller, which has the request context to return a meaningful `400`.
 */
export class QueryEventsDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @IsISO8601()
  from!: string;

  @IsISO8601()
  to!: string;

  /**
   * Page size: how many events to return per request. Capped to bound the work
   * (and the fan-out across buckets) a single read can do.
   */
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(1000)
  limit: number = 100;

  /**
   * Opaque pagination cursor returned as `nextCursor` by a previous call. When
   * present, the read resumes exactly where the prior page ended.
   */
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  cursor?: string;
}
