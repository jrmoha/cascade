import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { DeadLetter, RAW_EVENTS_TOPIC, rawEventSchema } from '@cascade/contracts';
import { RawEventRepository } from './raw-event.repository';
import { DeadLetterPublisher } from './dead-letter.publisher';

/** Bounded retry policy for transient (persistence) failures — see ADR-0006. */
export const MAX_ATTEMPTS = 3;
/** Exponential backoff base; waits are RETRY_BASE_MS * 2^(n-1) → 200ms, 400ms. */
export const RETRY_BASE_MS = 200;

@Controller()
export class ProcessorController {
  private readonly logger = new Logger(ProcessorController.name);

  constructor(
    private readonly repository: RawEventRepository,
    private readonly deadLetters: DeadLetterPublisher,
  ) {}

  /**
   * Consume raw events and append them to Cassandra (KAN-18), with dead-letter
   * handling for failures (KAN-23).
   *
   * - **Validation/deserialization failure** (the message isn't a valid
   *   `RawEvent`): permanent — dead-lettered immediately, no retry.
   * - **Persistence failure** (Cassandra write throws): transient — retried up
   *   to {@link MAX_ATTEMPTS} times with exponential backoff, then dead-lettered.
   *
   * The handler never rethrows: a poison message is routed to the DLQ and the
   * offset commits, so it can't crash the consumer or block the partition.
   * Successful writes are idempotent upserts, safe under at-least-once delivery.
   */
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

    // Transient failure: retry the write with bounded exponential backoff.
    let lastError: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        await this.repository.insert(event);
        this.logger.debug(
          `Persisted event ${event.eventId} (schema v${event.schemaVersion}) for project ${event.projectId}`,
        );
        return;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `Persist attempt ${attempt}/${MAX_ATTEMPTS} failed for event ${event.eventId}: ${(err as Error).message}`,
        );
        if (attempt < MAX_ATTEMPTS) {
          await sleep(RETRY_BASE_MS * 2 ** (attempt - 1));
        }
      }
    }

    // Retries exhausted — dead-letter the (valid) event for later replay.
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
