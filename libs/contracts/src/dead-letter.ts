import { z } from 'zod';
import { rawEventSchema } from './events';

/**
 * Dead-letter topic for events that fail *downstream* processing in the
 * Ingestion-Processor (KAN-23) — distinct from edge validation at the Collector
 * (KAN-22), which rejects bad input synchronously with a 400.
 */
export const RAW_EVENTS_DLQ_TOPIC = 'raw-events.dlq';

/**
 * Why a message was dead-lettered:
 * - `validation`  — the message could not be deserialized / failed the
 *   `rawEventSchema` contract. Permanent; not retried.
 * - `persistence` — a valid event that the Cassandra write kept rejecting after
 *   the bounded retry policy was exhausted. Transient in nature.
 */
export const deadLetterKindSchema = z.enum(['validation', 'persistence']);
export type DeadLetterKind = z.infer<typeof deadLetterKindSchema>;

/**
 * The envelope written to {@link RAW_EVENTS_DLQ_TOPIC}. It carries enough
 * context to inspect *and replay* a failed message: the raw original value
 * (preserved verbatim so even un-parseable messages can be recovered), the
 * parsed event when it was valid, the failure reason and kind, how many
 * attempts were made, when it failed, and where it came from on the source
 * topic.
 */
export const deadLetterSchema = z
  .object({
    /** The original Kafka message value, verbatim, so a replay loses nothing. */
    originalValue: z.string(),

    /** The parsed event — present only when the message passed validation (a persistence failure). */
    originalEvent: rawEventSchema.optional(),

    error: z
      .object({
        kind: deadLetterKindSchema,
        reason: z.string(),
      })
      .strict(),

    /** Number of processing attempts made before dead-lettering (>= 1). */
    attempts: z.number().int().positive(),

    /** When the message was dead-lettered (ISO-8601). */
    failedAt: z.string().datetime({ offset: true }),

    /** Where the message came from on the source topic, for inspection/replay. */
    source: z
      .object({
        topic: z.string(),
        partition: z.number().int().nonnegative(),
        offset: z.string(),
        key: z.string().nullable(),
      })
      .strict(),
  })
  .strict();

export type DeadLetter = z.infer<typeof deadLetterSchema>;
