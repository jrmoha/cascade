import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { RawEvent, recentHourlyWindows } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const SELECT_RAW_EVENTS = `
  SELECT project_id, time_window, event_id, type, event_time, payload
  FROM ${KEYSPACE}.raw_events
  WHERE project_id = ? AND time_window = ?`;

/**
 * Maps a Cassandra row back to the wire `RawEvent` shape produced by the
 * Collector, so the read path round-trips the same envelope a client POSTed.
 */
function toRawEvent(row: types.Row): RawEvent {
  return {
    eventId: row.get('event_id').toString(),
    projectId: row.get('project_id'),
    type: row.get('type'),
    timestamp: new Date(row.get('event_time')).toISOString(),
    payload: JSON.parse(row.get('payload') ?? '{}'),
  };
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
   * partitions. Results are returned newest-first by event time.
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
      .sort((a, b) => b.timestamp.localeCompare(a.timestamp));
  }
}
