import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { DeadLetter, RAW_EVENTS_TOPIC, rawEventSchema } from '@cascade/contracts';
import { DeadLetterPublisher } from './dead-letter.publisher';

/**
 * The Aggregator's `raw-events` consumer (ADR-0015). It is the **second,
 * independent** consumer of the topic (own `cascade-aggregator` group), parallel
 * to the Ingestion-Processor.
 *
 * Phase 1 skeleton (KAN-31): this validates each message against the shared
 * `rawEventSchema` and dead-letters malformed ones, but **derives no read models
 * yet** — the counter/leaderboard/funnel/retention writes are follow-up tickets.
 * The structure (validate → DLQ-on-invalid → handle-valid; never rethrow) and
 * the idempotency contract are established here so the views slot in without
 * reshaping the consumer.
 *
 * Idempotency note (ADR-0015): delivery is at-least-once. When additive views
 * land, the valid-event branch will dedup by `event.eventId` (the contract's
 * idempotency key) before applying non-idempotent updates; until then the branch
 * is a deliberate no-op.
 */
@Controller()
export class AggregatorController {
  private readonly logger = new Logger(AggregatorController.name);

  constructor(private readonly deadLetters: DeadLetterPublisher) {}

  @EventPattern(RAW_EVENTS_TOPIC)
  async handleRawEvent(@Payload() message: unknown, @Ctx() context: KafkaContext): Promise<void> {
    const kafkaMessage = context.getMessage();
    const source: DeadLetter['source'] = {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: kafkaMessage.offset,
      key: kafkaMessage.key?.toString() ?? null,
    };

    // Permanent failure: the message does not satisfy the shared contract.
    // Dead-letter immediately (no retry) so it can't block the partition.
    const parsed = rawEventSchema.safeParse(message);
    if (!parsed.success) {
      await this.deadLetters.publish({
        originalValue: kafkaMessage.value?.toString() ?? '',
        error: { kind: 'validation', reason: parsed.error.message },
        attempts: 1,
        failedAt: new Date().toISOString(),
        source,
      });
      return;
    }

    const event = parsed.data;
    // Skeleton: no read-model derivation yet. Acknowledge by returning.
    this.logger.debug(
      `Consumed event ${event.eventId} (${event.type}) for project ${event.projectId} — no view derived (skeleton)`,
    );
  }
}
