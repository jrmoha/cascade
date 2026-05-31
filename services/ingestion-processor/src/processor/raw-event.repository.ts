import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { RawEvent, toHourlyBucket } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const INSERT_RAW_EVENT = `
  INSERT INTO ${KEYSPACE}.raw_events
    (project_id, time_bucket, occurred_at, event_id, type, received_at, payload, session_id, actor_id, source)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

@Injectable()
export class RawEventRepository {
  constructor(private readonly cassandra: CassandraService) {}

  /**
   * Persist a single raw event. The full primary key
   * ((project_id, time_bucket), occurred_at, event_id) makes this an idempotent
   * upsert: re-processing the same event (Kafka at-least-once) overwrites an
   * identical row rather than duplicating it (event_id breaks occurred_at ties).
   *
   * `time_bucket` buckets by `occurredAt` (event time), so late/out-of-order
   * events land in the partition for when they happened, not when they arrived.
   */
  async insert(event: RawEvent): Promise<void> {
    const params = [
      event.projectId,
      toHourlyBucket(event.occurredAt),
      new Date(event.occurredAt),
      types.Uuid.fromString(event.eventId),
      event.type,
      new Date(event.receivedAt),
      JSON.stringify(event.payload ?? {}),
      event.sessionId ?? null,
      event.actorId ?? null,
      event.source ?? null,
    ];

    await this.cassandra.execute(INSERT_RAW_EVENT, params, { prepare: true });
  }
}
