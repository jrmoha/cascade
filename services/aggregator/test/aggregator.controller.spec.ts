import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { KafkaContext } from '@nestjs/microservices';
import type { DeadLetter, RawEvent } from '@cascade/contracts';
import type { DeadLetterPublisher } from '../src/aggregation/dead-letter.publisher';
import type { DedupStore } from '../src/aggregation/dedup.store';
import type { EventCountsRepository } from '../src/aggregation/event-counts.repository';
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

describe('AggregatorController (event counts)', () => {
  let firstSight: ReturnType<typeof vi.fn>;
  let forget: ReturnType<typeof vi.fn>;
  let increment: ReturnType<typeof vi.fn>;
  let publish: ReturnType<typeof vi.fn>;
  let controller: AggregatorController;

  beforeEach(() => {
    firstSight = vi.fn().mockResolvedValue(true);
    forget = vi.fn().mockResolvedValue(undefined);
    increment = vi.fn().mockResolvedValue(undefined);
    publish = vi.fn().mockResolvedValue(undefined);
    controller = new AggregatorController(
      { firstSight, forget } as unknown as DedupStore,
      { increment } as unknown as EventCountsRepository,
      { publish } as unknown as DeadLetterPublisher,
    );
  });

  it('counts a first-seen valid event and does not dead-letter it', async () => {
    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));

    expect(firstSight).toHaveBeenCalledWith(validEvent.eventId);
    expect(increment).toHaveBeenCalledTimes(1);
    expect(increment).toHaveBeenCalledWith(validEvent);
    expect(publish).not.toHaveBeenCalled();
    expect(forget).not.toHaveBeenCalled();
  });

  it('skips a duplicate event (dedup no-op) — no counter write, no dead-letter', async () => {
    firstSight.mockResolvedValue(false); // already seen within the horizon

    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));

    expect(firstSight).toHaveBeenCalledWith(validEvent.eventId);
    expect(increment).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
  });

  it('dead-letters an invalid message immediately as a validation failure (before dedup)', async () => {
    await controller.handleRawEvent({ not: 'an-event' }, ctx('{"not":"an-event"}'));

    expect(firstSight).not.toHaveBeenCalled();
    expect(increment).not.toHaveBeenCalled();
    expect(publish).toHaveBeenCalledTimes(1);
    const dl = publish.mock.calls[0][0] as DeadLetter;
    expect(dl.error.kind).toBe('validation');
    expect(dl.attempts).toBe(1);
    expect(dl.originalEvent).toBeUndefined();
    expect(dl.originalValue).toBe('{"not":"an-event"}');
    expect(dl.source).toEqual({ topic: 'raw-events', partition: 0, offset: '42', key: 'game-1' });
  });

  it('retries a failing counter write, then forgets the dedup marker and dead-letters', async () => {
    increment.mockRejectedValue(new Error('cassandra down'));

    await controller.handleRawEvent(validEvent, ctx(JSON.stringify(validEvent)));

    expect(increment).toHaveBeenCalledTimes(3); // MAX_ATTEMPTS
    // Compensating delete so the uncounted event can be re-counted on replay.
    expect(forget).toHaveBeenCalledWith(validEvent.eventId);
    expect(publish).toHaveBeenCalledTimes(1);
    const dl = publish.mock.calls[0][0] as DeadLetter;
    expect(dl.error.kind).toBe('persistence');
    expect(dl.error.reason).toContain('cassandra down');
    expect(dl.attempts).toBe(3);
    expect(dl.originalEvent).toEqual(validEvent);
  });

  it('never throws out of the handler (a bad message cannot block the partition)', async () => {
    await expect(
      controller.handleRawEvent('not json at all', ctx('not json at all', null)),
    ).resolves.toBeUndefined();
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
