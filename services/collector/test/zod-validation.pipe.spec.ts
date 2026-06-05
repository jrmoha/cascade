import { BadRequestException } from '@nestjs/common';
import { describe, expect, it } from 'vitest';
import { collectEventSchema } from '@cascade/contracts';
import { ZodValidationPipe } from '../src/common/zod-validation.pipe';

describe('ZodValidationPipe', () => {
  const pipe = new ZodValidationPipe(collectEventSchema);

  it('returns the parsed, sanitised value for valid input', () => {
    const out = pipe.transform({
      type: 'level_complete',
      payload: { level: 3 },
      // server-stamped / unknown keys are stripped, not rejected — including a
      // client-supplied projectId, which is derived from the API key (KAN-30).
      receivedAt: '2026-05-30T15:16:50.200Z',
      projectId: 'someone-elses-project',
      rogue: 'nope',
    }) as Record<string, unknown>;

    expect(out).toMatchObject({
      type: 'level_complete',
      payload: { level: 3 },
    });
    expect(out.receivedAt).toBeUndefined();
    expect(out.projectId).toBeUndefined();
    expect(out.rogue).toBeUndefined();
  });

  it('throws a structured 400 listing each failing field', () => {
    let thrown: BadRequestException | undefined;
    try {
      pipe.transform({ type: 123, occurredAt: 'not-a-date' }); // both wrong-typed
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
    expect(fields).toContain('type');
    expect(fields).toContain('occurredAt');
    for (const e of body.errors) {
      expect(typeof e.reason).toBe('string');
      expect(e.reason.length).toBeGreaterThan(0);
    }
  });
});
