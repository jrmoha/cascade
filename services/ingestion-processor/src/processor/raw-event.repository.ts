import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { RawEvent, toHourlyWindow } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const INSERT_RAW_EVENT = `
  INSERT INTO ${KEYSPACE}.raw_events
    (project_id, time_window, event_id, type, occurred_at, received_at, payload, session_id, actor_id, source)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

@Injectable()
export class RawEventRepository {
  constructor(private readonly cassandra: CassandraService) {}

  /**
   * Persist a single raw event. The full primary key
   * ((project_id, time_window), event_id) makes this an idempotent upsert:
   * re-processing the same event (Kafka at-least-once) overwrites an identical
   * row rather than duplicating it.
   *
   * `time_window` buckets by `occurredAt` (event time), so late/out-of-order
   * events land in the partition for when they happened, not when they arrived.
   */
  async insert(event: RawEvent): Promise<void> {
    const params = [
      event.projectId,
      toHourlyWindow(event.occurredAt),
      types.Uuid.fromString(event.eventId),
      event.type,
      new Date(event.occurredAt),
      new Date(event.receivedAt),
      JSON.stringify(event.payload ?? {}),
      event.sessionId ?? null,
      event.actorId ?? null,
      event.source ?? null,
    ];

    await this.cassandra.execute(INSERT_RAW_EVENT, params, { prepare: true });
  }
}
