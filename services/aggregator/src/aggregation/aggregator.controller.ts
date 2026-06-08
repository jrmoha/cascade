import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { DeadLetter, RAW_EVENTS_TOPIC, rawEventSchema } from '@cascade/contracts';
import { DeadLetterPublisher } from './dead-letter.publisher';
import { DedupStore } from './dedup.store';
import { EventCountsRepository } from './event-counts.repository';
import { LeaderboardRepository } from './leaderboard.repository';

/** Bounded retry policy for transient (counter-write) failures — see ADR-0006. */
export const MAX_ATTEMPTS = 3;
/** Exponential backoff base; waits are RETRY_BASE_MS * 2^(n-1) → 200ms, 400ms. */
export const RETRY_BASE_MS = 200;

/**
 * The Aggregator's `raw-events` consumer (ADR-0015). It is the **second,
 * independent** consumer of the topic (own `cascade-aggregator` group), parallel
 * to the Ingestion-Processor.
 *
 * As of KAN-32 it derives per-`(project, eventType, time-bucket)` **event
 * counts** (minute + hour) in Cassandra, and as of KAN-34 a per-project **live
 * leaderboard** in Redis sorted sets (ADR-0015 §2). Both are applied in the same
 * valid-event branch; funnels/retention are follow-up tickets that slot in the
 * same way.
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
    private readonly leaderboard: LeaderboardRepository,
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

    // Transient failure: derive the views with bounded exponential backoff. The
    // counter `+1` (additive) and the leaderboard `ZADD GT` (best-score) each get
    // their OWN retry, so a leaderboard hiccup never re-runs the non-idempotent
    // counter increment. If EITHER ultimately fails, undo the dedup marker (so the
    // event can be re-derived on a later replay) and dead-letter it.
    try {
      await this.withRetry('Count', event.eventId, () => this.counts.increment(event));
      await this.withRetry('Leaderboard', event.eventId, () => this.leaderboard.apply(event));
    } catch (err) {
      await this.dedup.forget(event.eventId);
      await this.deadLetters.publish({
        originalValue,
        originalEvent: event,
        error: { kind: 'persistence', reason: (err as Error)?.message ?? String(err) },
        attempts: MAX_ATTEMPTS,
        failedAt: new Date().toISOString(),
        source,
      });
      return;
    }

    this.logger.debug(
      `Derived views for event ${event.eventId} (${event.type}) for project ${event.projectId}`,
    );
  }

  /**
   * Run one view-write with bounded exponential backoff. Returns on the first
   * success; rethrows the last error once {@link MAX_ATTEMPTS} is exhausted so the
   * caller can dead-letter the event.
   */
  private async withRetry(label: string, eventId: string, op: () => Promise<void>): Promise<void> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        await op();
        return;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `${label} attempt ${attempt}/${MAX_ATTEMPTS} failed for event ${eventId}: ${(err as Error).message}`,
        );
        if (attempt < MAX_ATTEMPTS) {
          await sleep(RETRY_BASE_MS * 2 ** (attempt - 1));
        }
      }
    }
    throw lastError;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
