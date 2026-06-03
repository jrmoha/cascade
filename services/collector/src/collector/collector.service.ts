import { randomUUID } from 'node:crypto';
import { Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  CollectEventInput,
  RAW_EVENT_SCHEMA_VERSION,
  RAW_EVENTS_TOPIC,
  RawEvent,
  rawEventSchema,
} from '@cascade/contracts';
import { lastValueFrom } from 'rxjs';
import { KAFKA_PRODUCER } from './kafka.tokens';
import { ProjectSchemaClient } from '../ingest/project-schema.client';

@Injectable()
export class CollectorService implements OnApplicationBootstrap {
  private readonly logger = new Logger(CollectorService.name);

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly client: ClientKafka,
    private readonly projectSchema: ProjectSchemaClient,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.client.connect();
    this.logger.log('Kafka producer connected');
  }

  /**
   * Enrich the inbound event into the canonical RawEvent envelope and publish
   * it to `raw-events`, keyed by projectId so a project's events keep ordering
   * and land on the same partition.
   *
   * `projectId` is the authenticated project resolved from the API key (KAN-30),
   * not anything the client sent. Before building the envelope we validate the
   * payload against that project's registered JSON Schema for this event type —
   * an unregistered type is a `422`, a bad payload a `400` (both thrown by
   * {@link ProjectSchemaClient}).
   *
   * `receivedAt` is stamped here (ingest time); `occurredAt` (event time) is
   * taken from the client, defaulting to ingest time when absent. The envelope
   * is validated against the shared `rawEventSchema` before it leaves the
   * Collector, so nothing invalid reaches Kafka.
   *
   * @returns the eventId stamped on the published event.
   */
  async collect(projectId: string, input: CollectEventInput): Promise<string> {
    const payload = input.payload ?? {};
    await this.projectSchema.validatePayload(projectId, input.type, payload);

    const receivedAt = new Date().toISOString();

    const event: RawEvent = rawEventSchema.parse({
      eventId: randomUUID(),
      projectId,
      schemaVersion: RAW_EVENT_SCHEMA_VERSION,
      type: input.type,
      occurredAt: input.occurredAt ?? receivedAt,
      receivedAt,
      payload,
      sessionId: input.sessionId,
      actorId: input.actorId,
      source: input.source,
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
