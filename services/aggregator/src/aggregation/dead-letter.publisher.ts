import { Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { DeadLetter, RAW_EVENTS_DLQ_TOPIC, deadLetterSchema } from '@cascade/contracts';
import { lastValueFrom } from 'rxjs';
import { DLQ_PRODUCER } from './kafka.tokens';

/**
 * Publishes messages the Aggregator could not process to the dead-letter topic
 * (`raw-events.dlq`). The message is validated against the shared
 * `deadLetterSchema` before it is produced, so the DLQ only ever holds
 * well-formed, replayable records. Mirrors the Ingestion-Processor's publisher
 * (ADR-0006) — both consumers share the same DLQ contract and topic.
 *
 * Keyed by the source partition key (the original `projectId`) when present, so
 * a project's failures keep the same partition affinity as its events.
 */
@Injectable()
export class DeadLetterPublisher implements OnApplicationBootstrap {
  private readonly logger = new Logger(DeadLetterPublisher.name);

  constructor(@Inject(DLQ_PRODUCER) private readonly client: ClientKafka) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.client.connect();
    this.logger.log('DLQ producer connected');
  }

  async publish(deadLetter: DeadLetter): Promise<void> {
    const message = deadLetterSchema.parse(deadLetter);

    await lastValueFrom(
      this.client.emit(RAW_EVENTS_DLQ_TOPIC, {
        key: message.source.key ?? message.originalEvent?.projectId ?? null,
        value: JSON.stringify(message),
      }),
    );

    this.logger.warn(
      `Dead-lettered ${message.source.topic}@${message.source.partition}:${message.source.offset} ` +
        `(${message.error.kind}, ${message.attempts} attempt(s)): ${message.error.reason}`,
    );
  }
}
