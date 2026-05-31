import { describe, expect, it } from 'vitest';
import { rawEventSchema, type RawEvent } from '../src/events';

const valid = {
  eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
  projectId: 'game-1',
  type: 'level_complete',
  occurredAt: '2026-05-30T15:16:50.165Z',
  receivedAt: '2026-05-30T15:16:50.200Z',
  payload: { level: 3 },
} satisfies RawEvent;

describe('rawEventSchema', () => {
  it('accepts a well-formed envelope', () => {
    expect(rawEventSchema.parse(valid)).toEqual(valid);
  });

  it('defaults payload to {} when omitted', () => {
    const input: Record<string, unknown> = { ...valid };
    delete input.payload;
    expect(rawEventSchema.parse(input).payload).toEqual({});
  });

  it('separates occurredAt (event time) from receivedAt (ingest time)', () => {
    const parsed = rawEventSchema.parse(valid);
    expect(parsed.occurredAt).toBe('2026-05-30T15:16:50.165Z');
    expect(parsed.receivedAt).toBe('2026-05-30T15:16:50.200Z');
  });

  it('accepts the optional sessionId / actorId / source fields', () => {
    const parsed = rawEventSchema.parse({
      ...valid,
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
    expect(parsed.sessionId).toBe('sess-9');
    expect(parsed.actorId).toBe('player-42');
    expect(parsed.source).toBe('unity-sdk@1.4.0');
  });

  it('accepts a non-UTC offset for occurredAt', () => {
    expect(() =>
      rawEventSchema.parse({ ...valid, occurredAt: '2026-05-30T18:16:50.165+03:00' }),
    ).not.toThrow();
  });

  it('rejects a non-UUID eventId', () => {
    expect(() => rawEventSchema.parse({ ...valid, eventId: 'not-a-uuid' })).toThrow();
  });

  it('rejects a non-ISO occurredAt', () => {
    expect(() => rawEventSchema.parse({ ...valid, occurredAt: '30/05/2026' })).toThrow();
  });

  it('requires receivedAt', () => {
    const input: Record<string, unknown> = { ...valid };
    delete input.receivedAt;
    expect(() => rawEventSchema.parse(input)).toThrow();
  });

  it('requires a non-empty projectId and type', () => {
    expect(() => rawEventSchema.parse({ ...valid, projectId: '' })).toThrow();
    expect(() => rawEventSchema.parse({ ...valid, type: '' })).toThrow();
  });

  it('rejects unknown keys (strict envelope)', () => {
    expect(() => rawEventSchema.parse({ ...valid, rogue: 'nope' })).toThrow();
  });
});
