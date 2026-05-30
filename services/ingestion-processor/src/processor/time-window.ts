/**
 * Map an ISO-8601 timestamp to its hourly partition bucket in UTC, formatted
 * as 'YYYY-MM-DDTHH'. This is the `time_window` component of the Cassandra
 * partition key `(project_id, time_window)`, which bounds partition size.
 *
 * Falls back to the current hour if the timestamp is missing or unparseable,
 * so a malformed event still lands somewhere rather than being dropped.
 */
export function toHourlyWindow(iso: string | undefined): string {
  const date = iso ? new Date(iso) : new Date();
  const valid = Number.isNaN(date.getTime()) ? new Date() : date;
  return valid.toISOString().slice(0, 13); // 'YYYY-MM-DDTHH'
}
