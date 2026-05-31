import { describe, expect, it } from 'vitest';
import { RAW_EVENTS_DLQ_TOPIC, deadLetterSchema } from '../src/dead-letter';

const source = { topic: 'raw-events', partition: 0, offset: '42', key: 'game-1' };

describe('RAW_EVENTS_DLQ_TOPIC', () => {
  it('is the dotted dlq suffix of the raw-events topic', () => {
    expect(RAW_EVENTS_DLQ_TOPIC).toBe('raw-events.dlq');
  });
});

describe('deadLetterSchema', () => {
  it('accepts a validation dead-letter (no parsed event)', () => {
    const dl = {
      originalValue: 'not-json',
      error: { kind: 'validation', reason: 'Invalid input' },
      attempts: 1,
      failedAt: '2026-05-30T15:16:50.200Z',
      source,
    };
    expect(deadLetterSchema.parse(dl)).toEqual(dl);
  });

  it('accepts a persistence dead-letter carrying the parsed event', () => {
    const originalEvent = {
      eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
      projectId: 'game-1',
      type: 'level_complete',
      occurredAt: '2026-05-30T15:16:50.165Z',
      receivedAt: '2026-05-30T15:16:50.200Z',
      payload: { level: 3 },
    };
    const dl = {
      originalValue: JSON.stringify(originalEvent),
      originalEvent,
      error: { kind: 'persistence', reason: 'Cassandra write timeout' },
      attempts: 3,
      failedAt: '2026-05-30T15:16:51.000Z',
      source: { ...source, key: null },
    };
    expect(deadLetterSchema.parse(dl)).toMatchObject({ attempts: 3, originalEvent });
  });

  it('rejects an unknown error kind', () => {
    expect(() =>
      deadLetterSchema.parse({
        originalValue: 'x',
        error: { kind: 'mystery', reason: 'x' },
        attempts: 1,
        failedAt: '2026-05-30T15:16:50.200Z',
        source,
      }),
    ).toThrow();
  });

  it('requires attempts to be a positive integer', () => {
    expect(() =>
      deadLetterSchema.parse({
        originalValue: 'x',
        error: { kind: 'validation', reason: 'x' },
        attempts: 0,
        failedAt: '2026-05-30T15:16:50.200Z',
        source,
      }),
    ).toThrow();
  });

  it('rejects unknown top-level keys (strict)', () => {
    expect(() =>
      deadLetterSchema.parse({
        originalValue: 'x',
        error: { kind: 'validation', reason: 'x' },
        attempts: 1,
        failedAt: '2026-05-30T15:16:50.200Z',
        source,
        rogue: true,
      }),
    ).toThrow();
  });
});
