/**
 * Kafka topic that carries raw, accepted events from the Collector.
 * Consumed independently by the Ingestion-Processor and the Aggregator.
 */
export const RAW_EVENTS_TOPIC = 'raw-events';

/**
 * The raw event envelope as produced by the Collector onto `raw-events`.
 *
 * Phase 0 (walking skeleton): the Collector performs only light validation and
 * stamps a server-side `eventId`/`timestamp` when absent. Real schema validation
 * lands in Phase 1.
 *
 * `eventId` is the stable idempotency key used downstream for dedup
 * (clustering key in Cassandra; see ADR-0001). The Kafka message key is
 * `projectId`, so all events for a project land on the same partition.
 */
export interface RawEvent {
  /** Server-stamped unique id (UUID v4). Used downstream for idempotent upsert. */
  eventId: string;

  /** Tenant/project identifier. Doubles as the Kafka partition key. */
  projectId: string;

  /** Event type discriminator, e.g. `level_complete`. */
  type: string;

  /** ISO-8601 timestamp of the event. Defaulted to ingestion time if omitted. */
  timestamp: string;

  /** Arbitrary event payload. Opaque to the Collector in Phase 0. */
  payload: Record<string, unknown>;
}
