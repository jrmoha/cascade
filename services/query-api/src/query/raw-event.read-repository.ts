import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { RawEvent, recentHourlyWindows } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const SELECT_RAW_EVENTS = `
  SELECT project_id, time_window, event_id, type, occurred_at, received_at,
         payload, session_id, actor_id, source
  FROM ${KEYSPACE}.raw_events
  WHERE project_id = ? AND time_window = ?`;

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
   * Read events for a project across the most recent `hours` hourly buckets.
   *
   * Each bucket is a full partition `(project_id, time_window)`, so we issue one
   * prepared single-partition SELECT per window and merge the results. This is
   * deliberately partition-key-bounded: we never `ALLOW FILTERING` across
   * partitions. Results are returned newest-first by event time (`occurredAt`).
   *
   * NOTE (Phase 0): reading raw Cassandra from the Query API is a temporary
   * walking-skeleton shortcut to close the ingest→store→read loop (KAN-19). The
   * target design serves pre-aggregated read models built by the Aggregator;
   * this raw read-back is removed in Phase 1. See ADR-0003.
   */
  async readRecent(projectId: string, hours: number): Promise<RawEvent[]> {
    const windows = recentHourlyWindows(new Date(), hours);

    const resultSets = await Promise.all(
      windows.map((timeWindow) =>
        this.cassandra.execute(SELECT_RAW_EVENTS, [projectId, timeWindow], { prepare: true }),
      ),
    );

    return resultSets
      .flatMap((rs) => rs.rows.map(toRawEvent))
      .sort((a, b) => b.occurredAt.localeCompare(a.occurredAt));
  }
}
