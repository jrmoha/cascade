import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { hourlyBucketRange, MAX_QUERY_BUCKETS, RawEvent } from '@cascade/contracts';
import { QueryEventsDto } from './dto/query-events.dto';
import { InvalidCursorError, RawEventReadRepository } from './raw-event.read-repository';

interface QueryResponse {
  projectId: string;
  from: string;
  to: string;
  count: number;
  events: RawEvent[];
  nextCursor?: string;
}

@Controller()
export class QueryController {
  constructor(private readonly repository: RawEventReadRepository) {}

  /**
   * Read a project's stored events within an inclusive event-time window
   * `[from, to]`, newest-first and cursor-paged (KAN-25). The window is mapped to
   * the hourly `time_bucket` partitions it covers and read one partition at a
   * time — never a cross-partition scan. This serves bounded raw event retrieval
   * (replay/audit/debug); aggregation queries are served separately from read
   * models. See ADR-0008.
   */
  @Get('query')
  async query(@Query() dto: QueryEventsDto): Promise<QueryResponse> {
    const { projectId, from, to, limit, cursor } = dto;

    // Validate the window here at the HTTP edge, where we can return a 400 with
    // context. `from <= to`, and the span must stay within the partition cap.
    const buckets = hourlyBucketRange(from, to);
    if (buckets.length === 0) {
      throw new BadRequestException('`from` must be the same as or before `to`');
    }
    if (buckets.length > MAX_QUERY_BUCKETS) {
      throw new BadRequestException(
        `Time window spans ${buckets.length} hourly buckets, exceeding the maximum of ${MAX_QUERY_BUCKETS} (7 days)`,
      );
    }

    try {
      const { events, nextCursor } = await this.repository.readWindow({
        projectId,
        from,
        to,
        limit,
        cursor,
      });
      return {
        projectId,
        from,
        to,
        count: events.length,
        events,
        ...(nextCursor && { nextCursor }),
      };
    } catch (err) {
      if (err instanceof InvalidCursorError) throw new BadRequestException(err.message);
      throw err;
    }
  }
}
