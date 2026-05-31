import { of } from 'rxjs';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RAW_EVENTS_TOPIC, RawEvent } from '@cascade/contracts';
import { CollectorService } from '../src/collector/collector.service';
import { CollectEventDto } from '../src/collector/dto/collect-event.dto';

describe('CollectorService', () => {
  let emit: ReturnType<typeof vi.fn>;
  let service: CollectorService;

  beforeEach(() => {
    emit = vi.fn().mockReturnValue(of({}));
    // Minimal ClientKafka stub — only emit/connect are exercised.
    const client = { emit, connect: vi.fn() } as unknown as ConstructorParameters<
      typeof CollectorService
    >[0];
    service = new CollectorService(client);
  });

  const baseDto = (): CollectEventDto =>
    Object.assign(new CollectEventDto(), {
      projectId: 'game-1',
      type: 'level_complete',
      payload: { level: 3 },
    });

  function publishedEvent(): { key: string; value: RawEvent } {
    expect(emit).toHaveBeenCalledTimes(1);
    const [topic, message] = emit.mock.calls[0];
    expect(topic).toBe(RAW_EVENTS_TOPIC);
    return { key: message.key, value: JSON.parse(message.value) as RawEvent };
  }

  it('produces to the raw-events topic keyed by projectId', async () => {
    await service.collect(baseDto());
    const { key, value } = publishedEvent();
    expect(key).toBe('game-1');
    expect(value.projectId).toBe('game-1');
    expect(value.type).toBe('level_complete');
    expect(value.payload).toEqual({ level: 3 });
  });

  it('generates a UUID eventId when absent', async () => {
    const returnedId = await service.collect(baseDto());
    const { value } = publishedEvent();
    expect(value.eventId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
    expect(value.eventId).toBe(returnedId);
  });

  it('stamps receivedAt at ingestion time', async () => {
    const before = Date.now();
    await service.collect(baseDto());
    const { value } = publishedEvent();
    const ts = Date.parse(value.receivedAt);
    expect(Number.isNaN(ts)).toBe(false);
    expect(ts).toBeGreaterThanOrEqual(before - 1000);
  });

  it('defaults occurredAt to receivedAt when the client omits it', async () => {
    await service.collect(baseDto());
    const { value } = publishedEvent();
    expect(value.occurredAt).toBe(value.receivedAt);
  });

  it('preserves a client-supplied occurredAt distinct from receivedAt', async () => {
    const dto = Object.assign(baseDto(), { occurredAt: '2024-01-01T00:00:00.000Z' });
    await service.collect(dto);
    const { value } = publishedEvent();
    expect(value.occurredAt).toBe('2024-01-01T00:00:00.000Z');
    expect(value.receivedAt).not.toBe(value.occurredAt);
  });

  it('defaults payload to an empty object when absent', async () => {
    const dto = Object.assign(baseDto(), { payload: undefined });
    await service.collect(dto);
    const { value } = publishedEvent();
    expect(value.payload).toEqual({});
  });

  it('passes through the optional sessionId / actorId / source fields', async () => {
    const dto = Object.assign(baseDto(), {
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
    await service.collect(dto);
    const { value } = publishedEvent();
    expect(value.sessionId).toBe('sess-9');
    expect(value.actorId).toBe('player-42');
    expect(value.source).toBe('unity-sdk@1.4.0');
  });
});
