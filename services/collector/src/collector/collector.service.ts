import { randomUUID } from 'node:crypto';
import { Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { RAW_EVENTS_TOPIC, RawEvent, rawEventSchema } from '@cascade/contracts';
import { lastValueFrom } from 'rxjs';
import { CollectEventDto } from './dto/collect-event.dto';
import { KAFKA_PRODUCER } from './kafka.tokens';

@Injectable()
export class CollectorService implements OnApplicationBootstrap {
  private readonly logger = new Logger(CollectorService.name);

  constructor(@Inject(KAFKA_PRODUCER) private readonly client: ClientKafka) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.client.connect();
    this.logger.log('Kafka producer connected');
  }

  /**
   * Enrich the inbound event into the canonical RawEvent envelope and publish
   * it to `raw-events`, keyed by projectId so a project's events keep ordering
   * and land on the same partition.
   *
   * `receivedAt` is stamped here (ingest time); `occurredAt` (event time) is
   * taken from the client, defaulting to ingest time when absent. The envelope
   * is validated against the shared `rawEventSchema` before it leaves the
   * Collector, so nothing invalid reaches Kafka.
   *
   * @returns the eventId stamped on the published event.
   */
  async collect(dto: CollectEventDto): Promise<string> {
    const receivedAt = new Date().toISOString();

    const event: RawEvent = rawEventSchema.parse({
      eventId: randomUUID(),
      projectId: dto.projectId,
      type: dto.type,
      occurredAt: dto.occurredAt ?? receivedAt,
      receivedAt,
      payload: dto.payload ?? {},
      sessionId: dto.sessionId,
      actorId: dto.actorId,
      source: dto.source,
    });

    await lastValueFrom(
      this.client.emit(RAW_EVENTS_TOPIC, {
        key: event.projectId,
        value: JSON.stringify(event),
      }),
    );

    this.logger.debug(`Produced event ${event.eventId} for project ${event.projectId}`);
    return event.eventId;
  }
}
