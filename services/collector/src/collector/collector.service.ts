import { randomUUID } from 'node:crypto';
import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  CollectEventInput,
  RAW_EVENT_SCHEMA_VERSION,
  RAW_EVENTS_TOPIC,
  RawEvent,
  rawEventSchema,
} from '@cascade/contracts';
import { lastValueFrom, timeout } from 'rxjs';
import { KAFKA_PRODUCER } from './kafka.tokens';
import { InFlightLimiter } from './in-flight-limiter';
import { ProjectSchemaClient } from '../ingest/project-schema.client';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';

@Injectable()
export class CollectorService implements OnApplicationBootstrap {
  private readonly logger = new Logger(CollectorService.name);
  private readonly inFlight: InFlightLimiter;
  private readonly produceTimeoutMs: number;
  private readonly maxAttempts: number;
  private readonly retryBaseMs: number;

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly client: ClientKafka,
    private readonly projectSchema: ProjectSchemaClient,
    @Inject(APP_CONFIG) config: CollectorConfig,
  ) {
    this.inFlight = new InFlightLimiter(config.PRODUCE_MAX_INFLIGHT);
    this.produceTimeoutMs = config.PRODUCE_TIMEOUT_MS;
    this.maxAttempts = config.PRODUCE_MAX_ATTEMPTS;
    this.retryBaseMs = config.PRODUCE_RETRY_BASE_MS;
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.client.connect();
    this.logger.log('Kafka producer connected');
  }

  /**
   * Enrich the inbound event into the canonical RawEvent envelope and publish
   * it to `raw-events`, keyed by `sessionId ?? actorId ?? eventId` so a single
   * session's events keep ordering on one partition while a busy project's load
   * spreads across all partitions — the ordering guarantee the KAN-35 funnels
   * rely on, and the throughput scaling of KAN-40 (ADR-0020). `eventId` is always
   * present, so the key is never undefined; the per-project analytics stay correct
   * because every Aggregator write is commutative/idempotent (ADR-0016).
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
   * The produce itself is guarded for resilience (KAN-42, ADR-0021): bounded
   * in-flight backpressure, a per-attempt timeout, and bounded-backoff retry —
   * see {@link produce}.
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

    await this.produce(event);

    this.logger.debug(`Produced event ${event.eventId} for project ${event.projectId}`);
    return event.eventId;
  }

  /**
   * Publish one event with backpressure + bounded retry (KAN-42, ADR-0021).
   *
   * Backpressure first: reserve one of `PRODUCE_MAX_INFLIGHT` slots, or shed the
   * request with `503` immediately rather than buffering unboundedly. Then retry
   * a transient produce up to `PRODUCE_MAX_ATTEMPTS` times with exponential
   * backoff, each attempt bounded by `PRODUCE_TIMEOUT_MS` so a stuck broker can't
   * hang the request. If every attempt fails we return `503` — the event was
   * never acknowledged to the client, so a retry is safe and nothing is dropped.
   */
  private async produce(event: RawEvent): Promise<void> {
    if (!this.inFlight.tryAcquire()) {
      this.logger.warn(`Backpressure: shedding event ${event.eventId} (in-flight cap reached)`);
      throw new ServiceUnavailableException('Ingest is at capacity; retry shortly');
    }

    try {
      let lastError: unknown;
      for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
        try {
          await lastValueFrom(
            this.client
              .emit(RAW_EVENTS_TOPIC, {
                key: event.sessionId ?? event.actorId ?? event.eventId,
                value: JSON.stringify(event),
              })
              .pipe(timeout(this.produceTimeoutMs)),
          );
          return;
        } catch (err) {
          lastError = err;
          this.logger.warn(
            `Produce attempt ${attempt}/${this.maxAttempts} for event ${event.eventId} failed: ${(err as Error).message}`,
          );
          if (attempt < this.maxAttempts) {
            await sleep(this.retryBaseMs * 2 ** (attempt - 1));
          }
        }
      }

      this.logger.error(
        `Produce exhausted ${this.maxAttempts} attempts for event ${event.eventId}: ${(lastError as Error)?.message ?? String(lastError)}`,
      );
      throw new ServiceUnavailableException(
        'Unable to enqueue event after retries; retry the request',
      );
    } finally {
      this.inFlight.release();
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
