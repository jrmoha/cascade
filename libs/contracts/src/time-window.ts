/**
 * Hourly partition-bucket helpers shared by the write path (Ingestion-Processor,
 * which stamps `time_window` on each row) and the read path (Query API, which
 * must enumerate the same buckets to read events back). Keeping these in one
 * place guarantees both sides agree on the `(project_id, time_window)` partition
 * key — if they drifted, reads would miss rows.
 */

const HOUR_MS = 60 * 60 * 1000;

/**
 * Map an ISO-8601 timestamp to its hourly partition bucket in UTC, formatted as
 * 'YYYY-MM-DDTHH'. This is the `time_window` component of the Cassandra
 * partition key `(project_id, time_window)`, which bounds partition size. The
 * write path buckets by event time (`occurredAt`), so late/out-of-order events
 * land in the bucket for when they happened, not when they arrived.
 *
 * Falls back to the current hour if the timestamp is missing or unparseable, so
 * a malformed event still lands somewhere rather than being dropped.
 */
export function toHourlyWindow(iso: string | undefined): string {
  const date = iso ? new Date(iso) : new Date();
  const valid = Number.isNaN(date.getTime()) ? new Date() : date;
  return valid.toISOString().slice(0, 13); // 'YYYY-MM-DDTHH'
}

/**
 * Enumerate the hourly buckets to scan for a read-back, most-recent first:
 * the bucket containing `now` plus the preceding `hours - 1` buckets.
 *
 * `hours` is clamped to at least 1 (so the current bucket is always included).
 * The Query API issues one single-partition SELECT per returned window and
 * merges the results — this stays partition-key-bounded and avoids a
 * cross-partition `ALLOW FILTERING` scan.
 */
export function recentHourlyWindows(now: Date | string | undefined, hours: number): string[] {
  const base = now ? new Date(now) : new Date();
  const anchor = Number.isNaN(base.getTime()) ? new Date() : base;
  const count = Number.isFinite(hours) ? Math.max(1, Math.floor(hours)) : 1;

  const windows: string[] = [];
  for (let i = 0; i < count; i++) {
    windows.push(new Date(anchor.getTime() - i * HOUR_MS).toISOString().slice(0, 13));
  }
  return windows;
}
