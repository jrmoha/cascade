import { describe, expect, it } from 'vitest';
import { collectEventSchema, rawEventSchema, type RawEvent } from '../src/events';

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

describe('collectEventSchema (client input, derived from rawEventSchema)', () => {
  const input = {
    projectId: 'game-1',
    type: 'level_complete',
    occurredAt: '2026-05-30T15:16:50.165Z',
    payload: { level: 3 },
  };

  it('accepts a minimal valid input', () => {
    expect(collectEventSchema.parse(input)).toMatchObject(input);
  });

  it('makes occurredAt optional and defaults payload to {}', () => {
    const parsed = collectEventSchema.parse({ projectId: 'game-1', type: 'ping' });
    expect(parsed.occurredAt).toBeUndefined();
    expect(parsed.payload).toEqual({});
  });

  it('strips server-stamped fields (eventId, receivedAt) instead of rejecting them', () => {
    const parsed = collectEventSchema.parse({
      ...input,
      eventId: '8e8275f3-7874-43df-bbbf-f1a73a1aeb06',
      receivedAt: '2026-05-30T15:16:50.200Z',
    }) as Record<string, unknown>;
    expect(parsed.eventId).toBeUndefined();
    expect(parsed.receivedAt).toBeUndefined();
  });

  it('strips other unknown keys instead of rejecting them', () => {
    const parsed = collectEventSchema.parse({ ...input, rogue: 'nope' }) as Record<string, unknown>;
    expect(parsed.rogue).toBeUndefined();
  });

  it('keeps the optional sessionId / actorId / source fields', () => {
    const parsed = collectEventSchema.parse({
      ...input,
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
    expect(parsed).toMatchObject({
      sessionId: 'sess-9',
      actorId: 'player-42',
      source: 'unity-sdk@1.4.0',
    });
  });

  it('rejects a missing required field (projectId)', () => {
    expect(() => collectEventSchema.parse({ type: 'level_complete' })).toThrow();
  });

  it('rejects a wrong-typed required field (projectId as number)', () => {
    expect(() => collectEventSchema.parse({ ...input, projectId: 123 })).toThrow();
  });

  it('rejects a wrong-typed occurredAt', () => {
    expect(() => collectEventSchema.parse({ ...input, occurredAt: 'not-a-date' })).toThrow();
  });
});
