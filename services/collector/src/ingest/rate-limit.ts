import { createHash } from 'node:crypto';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type Redis from 'ioredis';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';
import { REDIS_CLIENT } from '../redis/redis.tokens';

/** Outcome of a single bucket check. */
export interface RateLimitResult {
  allowed: boolean;
  /** Milliseconds until the next token is available (only meaningful when denied). */
  retryAfterMs: number;
}

/**
 * Atomic token-bucket rate limiter over Redis (KAN-42, ADR-0021).
 *
 * One bucket per API key, keyed by the **SHA-256 of the key** so no plaintext
 * secret ever lands in Redis (matching the ingest cache in
 * {@link ProjectSchemaClient}). Refill-and-consume is a single Lua `EVAL`, so
 * concurrent requests for the same key cannot race the check-then-decrement.
 *
 * The bucket refills at `RATE_LIMIT_REFILL_PER_SEC` tokens/sec up to a ceiling
 * of `RATE_LIMIT_BURST`; a request that finds fewer than one token is denied,
 * with the milliseconds until a token is available (surfaced as `Retry-After`).
 * Idle buckets expire (TTL = time to fully refill, doubled) so the keyspace
 * stays bounded.
 *
 * **Fails open.** A Redis error means we allow the request rather than let the
 * limiter itself drop good traffic — auth still fails closed (ADR-0013); the
 * limiter is a spike shield, not a security boundary.
 */
@Injectable()
export class RateLimiter {
  private readonly logger = new Logger(RateLimiter.name);
  private readonly refillPerSec: number;
  private readonly burst: number;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(APP_CONFIG) config: CollectorConfig,
  ) {
    this.refillPerSec = config.RATE_LIMIT_REFILL_PER_SEC;
    this.burst = config.RATE_LIMIT_BURST;
  }

  /** Consume one token for `apiKey`. Fails open (allows) if Redis is unreachable. */
  async consume(apiKey: string): Promise<RateLimitResult> {
    const key = `ratelimit:${sha256(apiKey)}`;
    try {
      const [allowed, retryAfterMs] = (await this.redis.eval(
        TOKEN_BUCKET_LUA,
        1,
        key,
        this.refillPerSec,
        this.burst,
        Date.now(),
        1,
      )) as [number, number];
      return { allowed: allowed === 1, retryAfterMs };
    } catch (err) {
      this.logger.warn(`Rate-limit check failed open for a key: ${(err as Error).message}`);
      return { allowed: true, retryAfterMs: 0 };
    }
  }
}

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

/**
 * Refill-then-consume one bucket atomically.
 * KEYS[1]=bucket · ARGV: [refillPerSec, capacity, nowMs, requested]
 * Returns {allowed(0|1), retryAfterMs}.
 */
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
local retry_after_ms = 0
if tokens >= requested then
  allowed = 1
  tokens = tokens - requested
else
  retry_after_ms = math.ceil(((requested - tokens) / rate) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local ttl = math.ceil((capacity / rate) * 2)
if ttl < 1 then ttl = 1 end
redis.call('EXPIRE', key, ttl)

return {allowed, retry_after_ms}
`;
