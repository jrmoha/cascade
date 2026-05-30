import { Injectable } from '@nestjs/common';
import { types } from 'cassandra-driver';
import { RawEvent, toHourlyWindow } from '@cascade/contracts';
import { CassandraService, KEYSPACE } from '../cassandra/cassandra.service';

const INSERT_RAW_EVENT = `
  INSERT INTO ${KEYSPACE}.raw_events
    (project_id, time_window, event_id, type, event_time, payload)
  VALUES (?, ?, ?, ?, ?, ?)`;

@Injectable()
export class RawEventRepository {
  constructor(private readonly cassandra: CassandraService) {}

  /**
   * Persist a single raw event. The full primary key
   * ((project_id, time_window), event_id) makes this an idempotent upsert:
   * re-processing the same event (Kafka at-least-once) overwrites an identical
   * row rather than duplicating it.
   */
  async insert(event: RawEvent): Promise<void> {
    const params = [
      event.projectId,
      toHourlyWindow(event.timestamp),
      types.Uuid.fromString(event.eventId),
      event.type,
      new Date(event.timestamp),
      JSON.stringify(event.payload ?? {}),
    ];

    await this.cassandra.execute(INSERT_RAW_EVENT, params, { prepare: true });
  }
}
