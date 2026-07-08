import { of } from 'rxjs';
import { BadRequestException } from '@nestjs/common';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CollectEventInput, RAW_EVENTS_TOPIC, RawEvent } from '@cascade/contracts';
import { CollectorService } from '../src/collector/collector.service';
import type { ProjectSchemaClient } from '../src/ingest/project-schema.client';

const PROJECT_ID = 'game-1';

describe('CollectorService', () => {
  let emit: ReturnType<typeof vi.fn>;
  let validatePayload: ReturnType<typeof vi.fn>;
  let service: CollectorService;

  beforeEach(() => {
    emit = vi.fn().mockReturnValue(of({}));
    validatePayload = vi.fn().mockResolvedValue(undefined);
    // Minimal ClientKafka stub — only emit/connect are exercised.
    const client = { emit, connect: vi.fn() } as unknown as ConstructorParameters<
      typeof CollectorService
    >[0];
    const projectSchema = { validatePayload } as unknown as ProjectSchemaClient;
    service = new CollectorService(client, projectSchema);
  });

  const baseInput = (): CollectEventInput => ({
    type: 'level_complete',
    payload: { level: 3 },
  });

  function publishedEvent(): { key: string; value: RawEvent } {
    expect(emit).toHaveBeenCalledTimes(1);
    const [topic, message] = emit.mock.calls[0];
    expect(topic).toBe(RAW_EVENTS_TOPIC);
    return { key: message.key, value: JSON.parse(message.value) as RawEvent };
  }

  it('produces to the raw-events topic with the derived projectId in the envelope', async () => {
    await service.collect(PROJECT_ID, baseInput());
    const { value } = publishedEvent();
    expect(value.projectId).toBe('game-1');
    expect(value.type).toBe('level_complete');
    expect(value.payload).toEqual({ level: 3 });
  });

  // Partition key = sessionId ?? actorId ?? eventId (KAN-40, ADR-0020) — per-session
  // ordering, load spread across partitions, and never undefined.
  it('keys the message by sessionId when present', async () => {
    const dto = Object.assign(baseInput(), { sessionId: 'sess-9', actorId: 'player-42' });
    await service.collect(PROJECT_ID, dto);
    expect(publishedEvent().key).toBe('sess-9');
  });

  it('falls back to actorId when there is no sessionId', async () => {
    const dto = Object.assign(baseInput(), { actorId: 'player-42' });
    await service.collect(PROJECT_ID, dto);
    expect(publishedEvent().key).toBe('player-42');
  });

  it('falls back to eventId when there is neither sessionId nor actorId', async () => {
    await service.collect(PROJECT_ID, baseInput());
    const { key, value } = publishedEvent();
    expect(key).toBe(value.eventId);
  });

  it('validates the payload against the project schema before producing', async () => {
    await service.collect(PROJECT_ID, baseInput());
    expect(validatePayload).toHaveBeenCalledWith('game-1', 'level_complete', { level: 3 });
    // Validation precedes production.
    expect(validatePayload.mock.invocationCallOrder[0]).toBeLessThan(
      emit.mock.invocationCallOrder[0],
    );
  });

  it('does not produce when payload validation fails', async () => {
    validatePayload.mockRejectedValueOnce(new BadRequestException('bad payload'));
    await expect(service.collect(PROJECT_ID, baseInput())).rejects.toBeInstanceOf(
      BadRequestException,
    );
    expect(emit).not.toHaveBeenCalled();
  });

  it('generates a UUID eventId when absent', async () => {
    const returnedId = await service.collect(PROJECT_ID, baseInput());
    const { value } = publishedEvent();
    expect(value.eventId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(value.eventId).toBe(returnedId);
  });

  it('stamps receivedAt at ingestion time', async () => {
    const before = Date.now();
    await service.collect(PROJECT_ID, baseInput());
    const { value } = publishedEvent();
    const ts = Date.parse(value.receivedAt);
    expect(Number.isNaN(ts)).toBe(false);
    expect(ts).toBeGreaterThanOrEqual(before - 1000);
  });

  it('defaults occurredAt to receivedAt when the client omits it', async () => {
    await service.collect(PROJECT_ID, baseInput());
    const { value } = publishedEvent();
    expect(value.occurredAt).toBe(value.receivedAt);
  });

  it('preserves a client-supplied occurredAt distinct from receivedAt', async () => {
    const dto = Object.assign(baseInput(), { occurredAt: '2024-01-01T00:00:00.000Z' });
    await service.collect(PROJECT_ID, dto);
    const { value } = publishedEvent();
    expect(value.occurredAt).toBe('2024-01-01T00:00:00.000Z');
    expect(value.receivedAt).not.toBe(value.occurredAt);
  });

  it('defaults payload to an empty object when absent (defensive)', async () => {
    // The pipe normally fills payload via the schema default; this guards the
    // service if ever called with an input that lacks it.
    const input = { type: 'level_complete' } as CollectEventInput;
    await service.collect(PROJECT_ID, input);
    const { value } = publishedEvent();
    expect(value.payload).toEqual({});
  });

  it('passes through the optional sessionId / actorId / source fields', async () => {
    const dto = Object.assign(baseInput(), {
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
    await service.collect(PROJECT_ID, dto);
    const { value } = publishedEvent();
    expect(value.sessionId).toBe('sess-9');
    expect(value.actorId).toBe('player-42');
    expect(value.source).toBe('unity-sdk@1.4.0');
  });
});
