import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import {
  type CountBucket,
  type CountsGranularity,
  bucketSpanCount,
  hourlyBucketRange,
  MAX_COUNTS_MINUTE_BUCKETS,
  MAX_QUERY_BUCKETS,
  minuteBucketRange,
} from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const TABLE: Record<CountsGranularity, string> = {
  minute: `${KEYSPACE}.event_counts_by_minute`,
  hour: `${KEYSPACE}.event_counts_by_hour`,
};

/** Per-granularity cap on how many `(project_id, time_bucket)` partitions one
 * read may span, so fan-out stays bounded regardless of raw-event volume. */
const MAX_BUCKETS: Record<CountsGranularity, number> = {
  minute: MAX_COUNTS_MINUTE_BUCKETS,
  hour: MAX_QUERY_BUCKETS,
};

/** Enumerate the buckets covering `[from, to]` for the given granularity. */
function bucketRange(granularity: CountsGranularity, from: string, to: string): string[] {
  return granularity === 'minute' ? minuteBucketRange(from, to) : hourlyBucketRange(from, to);
}

/** Inputs for a counts read (already edge-validated by the controller). */
export interface CountsRead {
  projectId: string;
  from: string;
  to: string;
  granularity: CountsGranularity;
  /** Optional single event-type filter. */
  type?: string;
}

/** Thrown when a window spans more partitions than the granularity's cap. The
 * controller pre-validates and maps this to a 400; it is a backstop here. */
export class BucketSpanError extends Error {
  constructor(
    readonly buckets: number,
    readonly max: number,
    readonly granularity: CountsGranularity,
  ) {
    super(`Time window spans ${buckets} ${granularity} buckets, exceeding the limit of ${max}`);
    this.name = 'BucketSpanError';
  }
}

/**
 * Reads the Aggregator's windowed event-count read models (KAN-32) to serve
 * `GET /counts` (KAN-36). It reads **only** the `event_counts_by_minute` /
 * `event_counts_by_hour` counter aggregate tables — never `raw_events`, the raw
 * write path (ADR-0015 §2 / ADR-0018). This is the read side of the CQRS
 * boundary: analytics is served from pre-aggregated views, so its cost is a
 * function of the requested window, not of total ingested volume.
 *
 * The window is mapped to the `(project_id, time_bucket)` partitions it covers
 * (newest-first, like {@link hourlyBucketRange} for raw retrieval) and each is
 * read with one prepared single-partition SELECT — never a cross-partition scan
 * and never `ALLOW FILTERING`. Each partition holds at most one row per event
 * type, so no pagination is needed.
 */
@Injectable()
export class CountsRepository {
  constructor(private readonly cassandra: CassandraService) {}

  async read({ projectId, from, to, granularity, type }: CountsRead): Promise<CountBucket[]> {
    // Check the span arithmetically *before* materializing the bucket list, so
    // an over-cap window throws in O(1) rather than allocating a huge array
    // (the controller already guards; this is the backstop for direct callers).
    const max = MAX_BUCKETS[granularity];
    const span = bucketSpanCount(from, to, granularity);
    if (span > max) {
      throw new BucketSpanError(span, max, granularity);
    }

    const buckets = bucketRange(granularity, from, to);
    const table = TABLE[granularity];
    const select = type
      ? `SELECT event_type, count FROM ${table} WHERE project_id = ? AND time_bucket = ? AND event_type = ?`
      : `SELECT event_type, count FROM ${table} WHERE project_id = ? AND time_bucket = ?`;

    const results: CountBucket[] = [];
    for (const bucket of buckets) {
      const params = type ? [projectId, bucket, type] : [projectId, bucket];
      const rs = await this.cassandra.execute(select, params, { prepare: true });
      for (const row of rs.rows) {
        results.push({
          bucket,
          eventType: row.get('event_type'),
          count: toCount(row.get('count')),
        });
      }
    }
    return results;
  }
}

/** Cassandra `counter` columns come back as `Long`; normalize to a JS number.
 * `Long` stringifies to its decimal value, so `Number(String(...))` is a
 * driver-version-agnostic conversion. */
function toCount(value: types.Long | number | null): number {
  if (value == null) return 0;
  return typeof value === 'number' ? value : Number(value.toString());
}
