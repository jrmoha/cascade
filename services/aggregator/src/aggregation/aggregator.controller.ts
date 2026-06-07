import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { DeadLetter, RAW_EVENTS_TOPIC, rawEventSchema } from '@cascade/contracts';
import { DeadLetterPublisher } from './dead-letter.publisher';
import { DedupStore } from './dedup.store';
import { EventCountsRepository } from './event-counts.repository';

/** Bounded retry policy for transient (counter-write) failures — see ADR-0006. */
export const MAX_ATTEMPTS = 3;
/** Exponential backoff base; waits are RETRY_BASE_MS * 2^(n-1) → 200ms, 400ms. */
export const RETRY_BASE_MS = 200;

/**
 * The Aggregator's `raw-events` consumer (ADR-0015). It is the **second,
 * independent** consumer of the topic (own `cascade-aggregator` group), parallel
 * to the Ingestion-Processor.
 *
 * As of KAN-32 it derives the first read model: per-`(project, eventType,
 * time-bucket)` **event counts** (minute + hour) in Cassandra, windowed by event
 * time. Leaderboards/funnels/retention are follow-up tickets that slot into the
 * same valid-event branch.
 *
 * Failure handling mirrors the Ingestion-Processor and the project's DLQ rule
 * (ADR-0006): a message that fails the shared contract is **permanent** —
 * dead-lettered immediately, no retry; a valid event whose counter write keeps
 * failing is **transient** — retried with bounded backoff, then dead-lettered.
 * The handler never rethrows, so a poison message can't block the partition.
 *
 * Idempotency (ADR-0015 §4): delivery is at-least-once and counter `+1` is not
 * replay-safe, so each event is gated on {@link DedupStore.firstSight} before the
 * increment — a redelivery within the lateness horizon is a no-op. On give-up the
 * dedup marker is cleared ({@link DedupStore.forget}) so the dead-lettered event
 * isn't falsely recorded as counted.
 */
@Controller()
export class AggregatorController {
  private readonly logger = new Logger(AggregatorController.name);

  constructor(
    private readonly dedup: DedupStore,
    private readonly counts: EventCountsRepository,
    private readonly deadLetters: DeadLetterPublisher,
  ) {}

  @EventPattern(RAW_EVENTS_TOPIC)
  async handleRawEvent(@Payload() message: unknown, @Ctx() context: KafkaContext): Promise<void> {
    const kafkaMessage = context.getMessage();
    const source: DeadLetter['source'] = {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: kafkaMessage.offset,
      key: kafkaMessage.key?.toString() ?? null,
    };
    const originalValue = kafkaMessage.value?.toString() ?? '';

    // Permanent failure: the message does not satisfy the shared contract.
    // Dead-letter immediately (no retry) so it can't block the partition.
    const parsed = rawEventSchema.safeParse(message);
    if (!parsed.success) {
      await this.deadLetters.publish({
        originalValue,
        error: { kind: 'validation', reason: parsed.error.message },
        attempts: 1,
        failedAt: new Date().toISOString(),
        source,
      });
      return;
    }

    const event = parsed.data;

    // Idempotency gate: count each eventId at most once over the lateness horizon.
    // A redelivery (Kafka at-least-once, rebalance, duplicate produce) is a no-op.
    const firstSight = await this.dedup.firstSight(event.eventId);
    if (!firstSight) {
      this.logger.debug(`Duplicate event ${event.eventId} within dedup horizon — skipped`);
      return;
    }

    // Transient failure: retry the counter write with bounded exponential backoff.
    let lastError: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        await this.counts.increment(event);
        this.logger.debug(
          `Counted event ${event.eventId} (${event.type}) for project ${event.projectId}`,
        );
        return;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `Count attempt ${attempt}/${MAX_ATTEMPTS} failed for event ${event.eventId}: ${(err as Error).message}`,
        );
        if (attempt < MAX_ATTEMPTS) {
          await sleep(RETRY_BASE_MS * 2 ** (attempt - 1));
        }
      }
    }

    // Retries exhausted — undo the dedup marker so the (uncounted) event can be
    // re-counted on a later replay, then dead-letter it for inspection.
    await this.dedup.forget(event.eventId);
    await this.deadLetters.publish({
      originalValue,
      originalEvent: event,
      error: { kind: 'persistence', reason: (lastError as Error)?.message ?? String(lastError) },
      attempts: MAX_ATTEMPTS,
      failedAt: new Date().toISOString(),
      source,
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
