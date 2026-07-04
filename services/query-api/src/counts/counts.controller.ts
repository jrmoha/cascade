import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import {
  type CountsResponse,
  hourlyBucketRange,
  MAX_COUNTS_MINUTE_BUCKETS,
  MAX_QUERY_BUCKETS,
  minuteBucketRange,
} from '@cascade/contracts';
import { CountsQueryDto } from './dto/counts-query.dto';
import { CountsService } from './counts.service';

@Controller('counts')
export class CountsController {
  constructor(private readonly counts: CountsService) {}

  /**
   * Per-bucket event counts for a `(projectId, [from, to])` at minute or hour
   * granularity (KAN-36). Served from the Aggregator's `event_counts_by_minute`
   * / `event_counts_by_hour` counter tables — never by scanning `raw_events`
   * (ADR-0015 / ADR-0018). Read cost is bounded by the requested window, not by
   * total ingested volume.
   *
   * Note: this view is eventually consistent — a freshly ingested event is
   * counted only after the Aggregator processes it (seconds), so a just-sent
   * event may not appear yet. That lag is expected behaviour, not a bug.
   */
  @Get()
  async query(@Query() dto: CountsQueryDto): Promise<CountsResponse> {
    const { projectId, from, to, granularity, type } = dto;

    // Validate the window at the HTTP edge, where we can return a 400 with
    // context: `from <= to`, and the span must stay within the granularity cap
    // so fan-out is bounded (never a cross-partition scan).
    const buckets =
      granularity === 'minute' ? minuteBucketRange(from, to) : hourlyBucketRange(from, to);
    if (buckets.length === 0) {
      throw new BadRequestException('`from` must be the same as or before `to`');
    }
    const max = granularity === 'minute' ? MAX_COUNTS_MINUTE_BUCKETS : MAX_QUERY_BUCKETS;
    if (buckets.length > max) {
      throw new BadRequestException(
        `Time window spans ${buckets.length} ${granularity} buckets, exceeding the maximum of ${max}`,
      );
    }

    return this.counts.compute({ projectId, from, to, granularity, type });
  }
}
