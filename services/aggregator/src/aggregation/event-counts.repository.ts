import { Injectable } from '@nestjs/common';
import { RawEvent, toHourlyBucket, toMinuteBucket } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const INCREMENT_MINUTE = `
  UPDATE ${KEYSPACE}.event_counts_by_minute
     SET count = count + 1
   WHERE project_id = ? AND time_bucket = ? AND event_type = ?`;

const INCREMENT_HOUR = `
  UPDATE ${KEYSPACE}.event_counts_by_hour
     SET count = count + 1
   WHERE project_id = ? AND time_bucket = ? AND event_type = ?`;

/**
 * Writes the per-`(project, eventType, time-bucket)` event-count read models
 * (ADR-0015 §2) — the Aggregator's first derived view. Mirrors the
 * Ingestion-Processor's `RawEventRepository` shape, but writes to the
 * Aggregator's **own** Cassandra counter tables (never the raw write path).
 *
 * Counts are windowed by **event time** (`occurredAt`): both buckets are derived
 * from it via the shared `@cascade/contracts` helpers, so a late/out-of-order
 * event lands in the minute/hour for when it happened (ADR-0004/ADR-0015 §3).
 *
 * Both granularities are maintained directly so reads are O(1) at each. The
 * `+1` updates are NOT idempotent on replay — the caller MUST gate this on
 * {@link DedupStore.firstSight} so each `eventId` is counted at most once
 * (ADR-0015 §4).
 */
@Injectable()
export class EventCountsRepository {
  constructor(private readonly cassandra: CassandraService) {}

  /** Increment the minute and hour counters for one event. */
  async increment(event: RawEvent): Promise<void> {
    const minuteBucket = toMinuteBucket(event.occurredAt);
    const hourBucket = toHourlyBucket(event.occurredAt);

    await this.cassandra.execute(INCREMENT_MINUTE, [event.projectId, minuteBucket, event.type], {
      prepare: true,
    });
    await this.cassandra.execute(INCREMENT_HOUR, [event.projectId, hourBucket, event.type], {
      prepare: true,
    });
  }
}
