import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { KafkaContext } from '@nestjs/microservices';
import type { DeadLetter, RawEvent } from '@cascade/contracts';
import type { DeadLetterPublisher } from '../src/aggregation/dead-letter.publisher';
import { AggregatorController } from '../src/aggregation/aggregator.controller';

const validEvent: RawEvent = {
  eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
  projectId: 'game-1',
  schemaVersion: 1,
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

describe('AggregatorController (skeleton consumer)', () => {
  let publish: ReturnType<typeof vi.fn>;
  let controller: AggregatorController;

  beforeEach(() => {
    publish = vi.fn().mockResolvedValue(undefined);
    controller = new AggregatorController({ publish } as unknown as DeadLetterPublisher);
  });

  it('consumes a valid event without dead-lettering it (no view derived yet)', async () => {
    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));
    expect(publish).not.toHaveBeenCalled();
  });

  it('dead-letters an invalid message immediately as a validation failure', async () => {
    await controller.handleRawEvent({ not: 'an-event' }, ctx('{"not":"an-event"}'));

    expect(publish).toHaveBeenCalledTimes(1);
    const dl = publish.mock.calls[0][0] as DeadLetter;
    expect(dl.error.kind).toBe('validation');
    expect(dl.attempts).toBe(1);
    expect(dl.originalEvent).toBeUndefined();
    expect(dl.originalValue).toBe('{"not":"an-event"}');
    expect(dl.source).toEqual({ topic: 'raw-events', partition: 0, offset: '42', key: 'game-1' });
  });

  it('never throws out of the handler (a bad message cannot block the partition)', async () => {
    await expect(
      controller.handleRawEvent('not json at all', ctx('not json at all', null)),
    ).resolves.toBeUndefined();
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
