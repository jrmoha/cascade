import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { RAW_EVENTS_TOPIC, rawEventSchema } from '@cascade/contracts';
import { RawEventRepository } from './raw-event.repository';

@Controller()
export class ProcessorController {
  private readonly logger = new Logger(ProcessorController.name);

  constructor(private readonly repository: RawEventRepository) {}

  /**
   * Consume raw events and append them to Cassandra. The Kafka transport
   * JSON-parses the message value; we then validate it against the shared
   * `rawEventSchema` (the canonical contract) before persisting.
   *
   * Invalid messages are logged and skipped rather than crashing the consumer —
   * with at-least-once delivery a poison message would otherwise be redelivered
   * forever. (A dead-letter topic is a later ticket.)
   *
   * Writes are idempotent (primary-key upsert), so at-least-once redelivery of a
   * valid event is safe.
   */
  @EventPattern(RAW_EVENTS_TOPIC)
  async handleRawEvent(@Payload() message: unknown): Promise<void> {
    const result = rawEventSchema.safeParse(message);
    if (!result.success) {
      this.logger.warn(`Skipping invalid event: ${result.error.message}`);
      return;
    }

    const event = result.data;
    await this.repository.insert(event);
    this.logger.debug(`Persisted event ${event.eventId} for project ${event.projectId}`);
  }
}
