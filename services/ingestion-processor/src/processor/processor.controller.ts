import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { RAW_EVENTS_TOPIC, RawEvent } from '@cascade/contracts';
import { RawEventRepository } from './raw-event.repository';

@Controller()
export class ProcessorController {
  private readonly logger = new Logger(ProcessorController.name);

  constructor(private readonly repository: RawEventRepository) {}

  /**
   * Consume raw events and append them to Cassandra. The Kafka transport
   * JSON-parses the message value into a RawEvent. Writes are idempotent
   * (primary-key upsert), so at-least-once redelivery is safe.
   */
  @EventPattern(RAW_EVENTS_TOPIC)
  async handleRawEvent(@Payload() event: RawEvent): Promise<void> {
    await this.repository.insert(event);
    this.logger.debug(`Persisted event ${event.eventId} for project ${event.projectId}`);
  }
}
