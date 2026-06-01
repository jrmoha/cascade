import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { hourlyBucketRange, MAX_QUERY_BUCKETS, RawEvent } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const SELECT_WINDOW = `
  SELECT project_id, time_bucket, event_id, type, occurred_at, received_at,
         payload, session_id, actor_id, source
  FROM ${KEYSPACE}.raw_events
  WHERE project_id = ? AND time_bucket = ? AND occurred_at >= ? AND occurred_at <= ?`;

/** Inputs for a single time-range page read. */
export interface ReadWindow {
  projectId: string;
  /** Inclusive lower bound on `occurredAt` (ISO-8601). */
  from: string;
  /** Inclusive upper bound on `occurredAt` (ISO-8601). */
  to: string;
  /** Page size — max events to return. */
  limit: number;
  /** Opaque cursor from a previous page's `nextCursor`, if continuing. */
  cursor?: string;
}

/** A page of events plus the cursor to fetch the next one (absent at window end). */
export interface EventPage {
  events: RawEvent[];
  nextCursor?: string;
}

/**
 * Decoded pagination cursor. Pins the read to a specific partition (`b`, the
 * hourly `time_bucket`) and, within it, Cassandra's native driver paging-state
 * (`p`). When `p` is absent the cursor means "start at the beginning of bucket
 * `b`" — used when a page boundary lands exactly on a bucket boundary.
 */
interface PageCursor {
  b: string;
  p?: string;
}

/** Thrown when a client supplies a cursor that is malformed or does not belong
 * to the requested `[from, to]` window. The controller maps this to a 400. */
export class InvalidCursorError extends Error {
  constructor() {
    super('Invalid or mismatched pagination cursor');
    this.name = 'InvalidCursorError';
  }
}

function encodeCursor(cursor: PageCursor): string {
  return Buffer.from(JSON.stringify(cursor), 'utf8').toString('base64url');
}

function decodeCursor(raw: string): PageCursor {
  try {
    const parsed = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8')) as unknown;
    if (parsed && typeof (parsed as PageCursor).b === 'string') return parsed as PageCursor;
  } catch {
    /* fall through */
  }
  throw new InvalidCursorError();
}

/**
 * Maps a Cassandra row back to the wire `RawEvent` shape produced by the
 * Collector, so the read path round-trips the same envelope a client POSTed.
 * Optional fields are only included when the column is non-null, so an event
 * sent without them reads back without them (rather than as explicit nulls).
 */
function toRawEvent(row: types.Row): RawEvent {
  const event: RawEvent = {
    eventId: row.get('event_id').toString(),
    projectId: row.get('project_id'),
    type: row.get('type'),
    occurredAt: new Date(row.get('occurred_at')).toISOString(),
    receivedAt: new Date(row.get('received_at')).toISOString(),
    payload: JSON.parse(row.get('payload') ?? '{}'),
  };

  const sessionId = row.get('session_id');
  const actorId = row.get('actor_id');
  const source = row.get('source');
  if (sessionId != null) event.sessionId = sessionId;
  if (actorId != null) event.actorId = actorId;
  if (source != null) event.source = source;

  return event;
}

@Injectable()
export class RawEventReadRepository {
  constructor(private readonly cassandra: CassandraService) {}

  /**
   * Read a project's events whose `occurredAt` falls in the inclusive window
   * `[from, to]`, newest-first, one page at a time (KAN-25, ADR-0008).
   *
   * The window is mapped to the hourly `(project_id, time_bucket)` partitions it
   * covers ({@link hourlyBucketRange}, newest-first) and read one partition at a
   * time with a prepared, `occurred_at`-bounded single-partition SELECT — never
   * a cross-partition scan and never `ALLOW FILTERING`. The table's
   * `CLUSTERING ORDER BY (occurred_at DESC, …)` returns each partition
   * newest-first, and buckets are walked newest-first, so concatenating them is
   * already globally ordered without any app-side sort.
   *
   * Pagination uses Cassandra's native driver paging-state, carried across calls
   * in an opaque cursor that also pins the current bucket (paging-state is
   * per-partition, so the cursor records which partition it belongs to). An
   * absent `nextCursor` means the window has been fully read; a present one does
   * not guarantee further rows (the next page may come back empty) — callers
   * stop when `nextCursor` is absent.
   *
   * @throws Error if the window spans more than `MAX_QUERY_BUCKETS` partitions
   * (callers should pre-validate); {@link InvalidCursorError} on a bad cursor.
   */
  async readWindow({ projectId, from, to, limit, cursor }: ReadWindow): Promise<EventPage> {
    const buckets = hourlyBucketRange(from, to);
    if (buckets.length > MAX_QUERY_BUCKETS) {
      throw new Error(
        `Time window spans ${buckets.length} buckets, exceeding the limit of ${MAX_QUERY_BUCKETS}`,
      );
    }

    const fromDate = new Date(from);
    const toDate = new Date(to);

    const decoded = cursor ? decodeCursor(cursor) : undefined;
    let startIndex = 0;
    if (decoded) {
      startIndex = buckets.indexOf(decoded.b);
      // A cursor that points outside the requested window is a client mismatch.
      if (startIndex === -1) throw new InvalidCursorError();
    }

    const events: RawEvent[] = [];

    for (let i = startIndex; i < buckets.length; i++) {
      const bucket = buckets[i];
      // Driver paging-state only applies when resuming the cursor's own bucket.
      const pageState = i === startIndex ? decoded?.p : undefined;
      const remaining = limit - events.length;

      const rs = await this.cassandra.execute(
        SELECT_WINDOW,
        [projectId, bucket, fromDate, toDate],
        {
          prepare: true,
          fetchSize: remaining,
          pageState,
        },
      );

      for (const row of rs.rows) events.push(toRawEvent(row));

      if (events.length >= limit) {
        // Page is full — decide where the next page resumes.
        if (rs.pageState) {
          // More rows remain in this same partition.
          return { events, nextCursor: encodeCursor({ b: bucket, p: rs.pageState }) };
        }
        if (i + 1 < buckets.length) {
          // Partition exhausted; resume at the start of the next bucket.
          return { events, nextCursor: encodeCursor({ b: buckets[i + 1] }) };
        }
        // Exhausted the last partition exactly — nothing more to read.
        return { events };
      }
      // events.length < limit → this partition is exhausted; fall through.
    }

    return { events };
  }
}
