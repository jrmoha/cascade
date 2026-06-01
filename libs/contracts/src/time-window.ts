/**
 * Hourly partition-bucket helpers shared by the write path (Ingestion-Processor,
 * which stamps `time_bucket` on each row) and the read path (Query API, which
 * must enumerate the same buckets to read events back). Keeping these in one
 * place guarantees both sides agree on the `(project_id, time_bucket)` partition
 * key — if they drifted, reads would miss rows. See ADR-0007.
 */

const HOUR_MS = 60 * 60 * 1000;

/**
 * Map an ISO-8601 timestamp to its hourly partition bucket in UTC, formatted as
 * 'YYYY-MM-DDTHH'. This is the `time_bucket` component of the Cassandra
 * partition key `(project_id, time_bucket)`, which bounds partition size. The
 * write path buckets by event time (`occurredAt`), so late/out-of-order events
 * land in the bucket for when they happened, not when they arrived.
 *
 * Falls back to the current hour if the timestamp is missing or unparseable, so
 * a malformed event still lands somewhere rather than being dropped.
 */
export function toHourlyBucket(iso: string | undefined): string {
  const date = iso ? new Date(iso) : new Date();
  const valid = Number.isNaN(date.getTime()) ? new Date() : date;
  return valid.toISOString().slice(0, 13); // 'YYYY-MM-DDTHH'
}

/**
 * The maximum number of hourly buckets a single time-range read may span
 * (168 = 7 days). Each bucket is one `(project_id, time_bucket)` partition the
 * Query API reads with a prepared single-partition SELECT, so this caps the
 * fan-out of one request to a bounded set of partitions — never an unbounded
 * cross-partition scan. The Query API rejects windows wider than this. See
 * ADR-0008.
 */
export const MAX_QUERY_BUCKETS = 168;

/**
 * Enumerate the hourly buckets covering the inclusive window `[from, to]`,
 * most-recent first (so it aligns with the table's `occurred_at DESC` clustering
 * order — newest events first). `from`/`to` are floored to their UTC hour, and
 * both endpoint hours are included.
 *
 * A window can straddle several `(project_id, time_bucket)` partitions; the
 * Query API reads each returned bucket with one prepared single-partition SELECT
 * and merges the results, staying partition-key-bounded and avoiding a
 * cross-partition `ALLOW FILTERING` scan (KAN-25, ADR-0008). Callers should
 * bound the span against {@link MAX_QUERY_BUCKETS}.
 *
 * Returns an empty array if `from` is after `to` or either date is unparseable.
 */
export function hourlyBucketRange(from: Date | string, to: Date | string): string[] {
  const fromDate = new Date(from);
  const toDate = new Date(to);
  if (Number.isNaN(fromDate.getTime()) || Number.isNaN(toDate.getTime())) return [];

  const floorToHour = (ms: number): number => Math.floor(ms / HOUR_MS) * HOUR_MS;
  const newest = floorToHour(toDate.getTime());
  const oldest = floorToHour(fromDate.getTime());

  const buckets: string[] = [];
  for (let ms = newest; ms >= oldest; ms -= HOUR_MS) {
    buckets.push(new Date(ms).toISOString().slice(0, 13));
  }
  return buckets;
}
