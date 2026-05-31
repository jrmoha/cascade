import { BadRequestException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { collectEventSchema } from '@cascade/contracts';
import { ZodValidationPipe } from '../src/common/zod-validation.pipe';

describe('ZodValidationPipe', () => {
  const pipe = new ZodValidationPipe(collectEventSchema);

  it('returns the parsed, sanitised value for valid input', () => {
    const out = pipe.transform({
      projectId: 'game-1',
      type: 'level_complete',
      payload: { level: 3 },
      // server-stamped / unknown keys are stripped, not rejected
      receivedAt: '2026-05-30T15:16:50.200Z',
      rogue: 'nope',
    }) as Record<string, unknown>;

    expect(out).toMatchObject({
      projectId: 'game-1',
      type: 'level_complete',
      payload: { level: 3 },
    });
    expect(out.receivedAt).toBeUndefined();
    expect(out.rogue).toBeUndefined();
  });

  it('throws a structured 400 listing each failing field', () => {
    let thrown: BadRequestException | undefined;
    try {
      pipe.transform({ type: 123 }); // projectId missing, type wrong-typed
    } catch (err) {
      thrown = err as BadRequestException;
    }

    expect(thrown).toBeInstanceOf(BadRequestException);
    const body = thrown!.getResponse() as {
      statusCode: number;
      message: string;
      errors: { field: string; reason: string }[];
    };
    expect(body.statusCode).toBe(400);
    expect(body.message).toBe('Event validation failed');
    const fields = body.errors.map((e) => e.field);
    expect(fields).toContain('projectId');
    expect(fields).toContain('type');
    for (const e of body.errors) {
      expect(typeof e.reason).toBe('string');
      expect(e.reason.length).toBeGreaterThan(0);
    }
  });
});
