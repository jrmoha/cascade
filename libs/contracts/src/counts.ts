import { z } from 'zod';

/**
 * Event-counts read contract (KAN-36, ADR-0015 §2 / ADR-0018). The Aggregator
 * maintains per-`(project, eventType, time-bucket)` counts in two Cassandra
 * `counter` aggregate tables — `event_counts_by_minute` and
 * `event_counts_by_hour` (KAN-32) — and the Query API's `GET /counts` serves a
 * time-series straight out of them, **never** by scanning `raw_events`. These
 * schemas are the single source of truth for the request granularity and the
 * response shape, shared by the Query API and its tests.
 *
 * The counts are windowed by **event time** (`occurredAt`), so a late or
 * out-of-order event contributes to the bucket for when it happened. Because a
 * fresh event is only counted after the Aggregator processes it, this view is
 * eventually consistent — a read can lag ingestion by the Aggregator's
 * processing latency (seconds). That lag is expected behaviour, not a bug.
 */

/**
 * Bucket granularity for a counts read. `minute` reads
 * `event_counts_by_minute` ('YYYY-MM-DDTHH:MM' buckets); `hour` reads
 * `event_counts_by_hour` ('YYYY-MM-DDTHH' buckets).
 */
export const countsGranularitySchema = z.enum(['minute', 'hour']);
export type CountsGranularity = z.infer<typeof countsGranularitySchema>;

/**
 * One bucket's count for one event type. `bucket` is the UTC time-bucket string
 * at the requested granularity; `count` is the number of events of `eventType`
 * that occurred within it.
 */
export const countBucketSchema = z.object({
  bucket: z.string(),
  eventType: z.string(),
  count: z.number().int().nonnegative(),
});
export type CountBucket = z.infer<typeof countBucketSchema>;

/**
 * `GET /counts` — per-bucket event counts for a `(projectId, [from, to])` at a
 * chosen `granularity`, optionally narrowed to a single `type`. `buckets` is
 * ordered most-recent first (mirroring the bucket walk), and only buckets with
 * at least one counted event appear.
 */
export const countsResponseSchema = z.object({
  projectId: z.string(),
  granularity: countsGranularitySchema,
  from: z.string(),
  to: z.string(),
  buckets: z.array(countBucketSchema),
});
export type CountsResponse = z.infer<typeof countsResponseSchema>;
