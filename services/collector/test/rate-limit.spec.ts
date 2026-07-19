import type { ExecutionContext } from '@nestjs/common';
import { HttpException } from '@nestjs/common';
import type Redis from 'ioredis';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CollectorConfig } from '../src/config/env.schema';
import { RateLimiter } from '../src/ingest/rate-limit';
import { RateLimitGuard } from '../src/ingest/rate-limit.guard';
import { API_KEY_HEADER } from '../src/ingest/api-key.guard';

const config = {
  RATE_LIMIT_REFILL_PER_SEC: 50,
  RATE_LIMIT_BURST: 100,
} as unknown as CollectorConfig;

describe('RateLimiter', () => {
  let evalFn: ReturnType<typeof vi.fn>;
  let limiter: RateLimiter;

  beforeEach(() => {
    evalFn = vi.fn();
    const redis = { eval: evalFn } as unknown as Redis;
    limiter = new RateLimiter(redis, config);
  });

  it('allows when the bucket returns a token', async () => {
    evalFn.mockResolvedValue([1, 0]);
    expect(await limiter.consume('cas_key.secret')).toEqual({ allowed: true, retryAfterMs: 0 });
  });

  it('denies with a retry hint when the bucket is empty', async () => {
    evalFn.mockResolvedValue([0, 420]);
    expect(await limiter.consume('cas_key.secret')).toEqual({ allowed: false, retryAfterMs: 420 });
  });

  it('buckets by the sha256 of the key (no plaintext secret in Redis)', async () => {
    evalFn.mockResolvedValue([1, 0]);
    await limiter.consume('cas_key.secret');
    const bucketKey = evalFn.mock.calls[0][2] as string; // EVAL(script, numKeys, KEY, ...)
    expect(bucketKey).toMatch(/^ratelimit:[0-9a-f]{64}$/);
    expect(bucketKey).not.toContain('secret');
  });

  it('fails open (allows) when Redis errors — the limiter is a shield, not a gate', async () => {
    evalFn.mockRejectedValue(new Error('redis down'));
    expect(await limiter.consume('cas_key.secret')).toEqual({ allowed: true, retryAfterMs: 0 });
  });
});

describe('RateLimitGuard', () => {
  function contextFor(headers: Record<string, string>): {
    ctx: ExecutionContext;
    setHeader: ReturnType<typeof vi.fn>;
  } {
    const setHeader = vi.fn();
    const ctx = {
      switchToHttp: () => ({
        getRequest: () => ({ headers }),
        getResponse: () => ({ setHeader }),
      }),
    } as unknown as ExecutionContext;
    return { ctx, setHeader };
  }

  it('passes through when there is no API key (ApiKeyGuard owns the 401)', async () => {
    const consume = vi.fn();
    const guard = new RateLimitGuard({ consume } as unknown as RateLimiter);
    const { ctx } = contextFor({});
    expect(await guard.canActivate(ctx)).toBe(true);
    expect(consume).not.toHaveBeenCalled();
  });

  it('allows a request that is under budget', async () => {
    const consume = vi.fn().mockResolvedValue({ allowed: true, retryAfterMs: 0 });
    const guard = new RateLimitGuard({ consume } as unknown as RateLimiter);
    const { ctx } = contextFor({ [API_KEY_HEADER]: 'cas_key.secret' });
    expect(await guard.canActivate(ctx)).toBe(true);
    expect(consume).toHaveBeenCalledWith('cas_key.secret');
  });

  it('throws 429 with a Retry-After header when over budget', async () => {
    const consume = vi.fn().mockResolvedValue({ allowed: false, retryAfterMs: 1500 });
    const guard = new RateLimitGuard({ consume } as unknown as RateLimiter);
    const { ctx, setHeader } = contextFor({ [API_KEY_HEADER]: 'cas_key.secret' });

    const err = await guard.canActivate(ctx).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(HttpException);
    expect((err as HttpException).getStatus()).toBe(429);
    expect(setHeader).toHaveBeenCalledWith('Retry-After', 2); // ceil(1500ms) = 2s
  });
});
