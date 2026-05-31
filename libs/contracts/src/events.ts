import { z } from 'zod';

/**
 * Kafka topic that carries raw, accepted events from the Collector.
 * Consumed independently by the Ingestion-Processor and the Aggregator.
 */
export const RAW_EVENTS_TOPIC = 'raw-events';

/**
 * The canonical event envelope every Cascade event conforms to — the single
 * shared contract across services (KAN-21). This Zod schema is the **one source
 * of truth**: the `RawEvent` TypeScript type is derived from it via `z.infer`,
 * so the static type and the runtime validator can never drift.
 *
 * Producers (Collector) validate before publishing to `raw-events`; consumers
 * (Ingestion-Processor) validate on receipt. The Kafka message key is
 * `projectId`, so all events for a project land on the same partition.
 *
 * `.strict()` rejects unknown keys, mirroring the Collector's HTTP-boundary
 * `forbidNonWhitelisted` behaviour and keeping the wire envelope closed.
 */
export const rawEventSchema = z
  .object({
    /**
     * Server-stamped unique id (UUID v4). The stable **idempotency key** used
     * downstream for dedup — it is the Cassandra clustering key, so a
     * re-delivered event (Kafka at-least-once) upserts rather than duplicates
     * (see ADR-0001).
     */
    eventId: z.string().uuid(),

    /** Tenant/project identifier. Doubles as the Kafka partition key. */
    projectId: z.string().min(1),

    /** Event type discriminator, e.g. `level_complete`. */
    type: z.string().min(1),

    /**
     * **Event time** — when the event happened, as reported by the client.
     * ISO-8601. Aggregation keys off this (not arrival order): late and
     * out-of-order events are normal in telemetry.
     */
    occurredAt: z.string().datetime({ offset: true }),

    /**
     * **Ingest time** — when the Collector accepted the event. ISO-8601.
     * Stamped server-side; distinct from `occurredAt` so we can reason about
     * ingestion lag and late arrivals.
     */
    receivedAt: z.string().datetime({ offset: true }),

    /** Arbitrary type-specific event body. Defaults to `{}` when omitted. */
    payload: z.record(z.unknown()).default({}),

    /** Optional: client session this event belongs to. */
    sessionId: z.string().min(1).optional(),

    /** Optional: the player/user the event is about. */
    actorId: z.string().min(1).optional(),

    /** Optional: emitting source / SDK version, e.g. `unity-sdk@1.4.0`. */
    source: z.string().min(1).optional(),
  })
  .strict();

/**
 * The canonical event envelope, inferred from {@link rawEventSchema}. Importing
 * this type and validating with the schema guarantees both sides of every
 * boundary agree on the same shape.
 */
export type RawEvent = z.infer<typeof rawEventSchema>;

/**
 * The client-supplied input accepted by the Collector's `POST /collect`,
 * **derived from {@link rawEventSchema}** so the ingest gate validates against
 * the one canonical contract rather than a re-implemented copy (KAN-22).
 *
 * Differences from the full envelope:
 * - `eventId` and `receivedAt` are omitted — they are stamped server-side. The
 *   schema `.strip()`s unknown keys, so a client that sends them (or any other
 *   stray field) has them silently ignored and re-stamped, rather than rejected.
 * - `occurredAt` is optional — the Collector defaults it to `receivedAt` when
 *   the client omits it. (`payload` is already optional via its default.)
 *
 * Missing or wrong-typed *required* fields (`projectId`, `type`) still fail
 * validation, so bad data never reaches the `raw-events` topic.
 */
export const collectEventSchema = rawEventSchema
  .omit({ eventId: true, receivedAt: true })
  .partial({ occurredAt: true })
  .strip();

/** The validated, client-supplied shape accepted by `POST /collect`. */
export type CollectEventInput = z.infer<typeof collectEventSchema>;
