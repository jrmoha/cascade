import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { KafkaContext } from '@nestjs/microservices';
import type { DeadLetter, RawEvent } from '@cascade/contracts';
import type { RawEventRepository } from '../src/processor/raw-event.repository';
import type { DeadLetterPublisher } from '../src/processor/dead-letter.publisher';
import { MAX_ATTEMPTS, ProcessorController } from '../src/processor/processor.controller';

const validEvent: RawEvent = {
  eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
  projectId: 'game-1',
  type: 'level_complete',
  occurredAt: '2026-05-30T15:16:50.165Z',
  receivedAt: '2026-05-30T15:16:50.200Z',
  payload: { level: 3 },
};

/** Minimal KafkaContext stand-in exposing the bits the handler reads. */
function ctx(value: string, key: string | null = 'game-1'): KafkaContext {
  return {
    getMessage: () => ({
      offset: '42',
      key: key === null ? null : Buffer.from(key),
      value: Buffer.from(value),
    }),
    getTopic: () => 'raw-events',
    getPartition: () => 0,
  } as unknown as KafkaContext;
}

describe('ProcessorController (dead-letter handling)', () => {
  let insert: ReturnType<typeof vi.fn>;
  let publish: ReturnType<typeof vi.fn>;
  let controller: ProcessorController;

  beforeEach(() => {
    insert = vi.fn().mockResolvedValue(undefined);
    publish = vi.fn().mockResolvedValue(undefined);
    controller = new ProcessorController(
      { insert } as unknown as RawEventRepository,
      { publish } as unknown as DeadLetterPublisher,
    );
  });

  it('persists a valid event and does not dead-letter it', async () => {
    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));
    expect(insert).toHaveBeenCalledTimes(1);
    expect(insert).toHaveBeenCalledWith(validEvent);
    expect(publish).not.toHaveBeenCalled();
  });

  it('dead-letters an invalid message immediately, without persisting or retrying', async () => {
    await controller.handleRawEvent({ not: 'an-event' }, ctx('{"not":"an-event"}'));

    expect(insert).not.toHaveBeenCalled();
    expect(publish).toHaveBeenCalledTimes(1);
    const dl = publish.mock.calls[0][0] as DeadLetter;
    expect(dl.error.kind).toBe('validation');
    expect(dl.attempts).toBe(1);
    expect(dl.originalEvent).toBeUndefined();
    expect(dl.originalValue).toBe('{"not":"an-event"}');
    expect(dl.source).toEqual({ topic: 'raw-events', partition: 0, offset: '42', key: 'game-1' });
  });

  it('retries a persistence failure MAX_ATTEMPTS times, then dead-letters the valid event', async () => {
    insert.mockRejectedValue(new Error('Cassandra unavailable'));

    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));

    expect(insert).toHaveBeenCalledTimes(MAX_ATTEMPTS);
    expect(publish).toHaveBeenCalledTimes(1);
    const dl = publish.mock.calls[0][0] as DeadLetter;
    expect(dl.error.kind).toBe('persistence');
    expect(dl.error.reason).toContain('Cassandra unavailable');
    expect(dl.attempts).toBe(MAX_ATTEMPTS);
    expect(dl.originalEvent).toEqual(validEvent);
  });

  it('recovers if a retry succeeds before the limit (no dead-letter)', async () => {
    insert.mockRejectedValueOnce(new Error('transient blip')).mockResolvedValueOnce(undefined);

    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));

    expect(insert).toHaveBeenCalledTimes(2);
    expect(publish).not.toHaveBeenCalled();
  });
});
