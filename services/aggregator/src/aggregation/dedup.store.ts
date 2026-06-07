import { Inject, Injectable } from '@nestjs/common';
import Redis from 'ioredis';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { REDIS_CLIENT } from '../redis/redis.tokens';

/** Redis key namespace for the per-`eventId` dedup guard. */
const DEDUP_KEY_PREFIX = 'aggregator:dedup:';

/**
 * Per-`eventId` dedup guard that keeps the Aggregator's **additive** counters
 * replay-safe under Kafka at-least-once delivery (ADR-0015 §4). A naive counter
 * `+1` would double-count on redelivery, which the project's "never double-count"
 * rule forbids; gating each increment on a first-sight check makes a redelivery
 * within the lateness horizon a no-op.
 *
 * Backed by a Redis key with a TTL sized to the horizon
 * (`AGGREGATOR_DEDUP_TTL_SECONDS`) — ADR-0015 lists this as the first-choice
 * dedup store. The store is bounded (keys expire) and, like every read model, is
 * a backstop only: the source of truth is the log, and a full replay from offset
 * 0 reconstructs counts exactly (ADR-0015 §5).
 */
@Injectable()
export class DedupStore {
  private readonly ttlSeconds: number;

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(APP_CONFIG) config: AggregatorConfig,
  ) {
    this.ttlSeconds = config.AGGREGATOR_DEDUP_TTL_SECONDS;
  }

  /**
   * Atomically record that `eventId` has been seen. Returns `true` the **first**
   * time (caller should apply the increment) and `false` for a redelivery within
   * the horizon (caller should skip). Uses `SET key val NX EX ttl`, which is a
   * single atomic op — safe across concurrently-processed partitions.
   */
  async firstSight(eventId: string): Promise<boolean> {
    const result = await this.redis.set(
      `${DEDUP_KEY_PREFIX}${eventId}`,
      '1',
      'EX',
      this.ttlSeconds,
      'NX',
    );
    return result === 'OK';
  }

  /**
   * Compensating delete: clear a dedup marker so a not-yet-counted event can be
   * counted on a later redelivery. Called when the counter write ultimately
   * fails (after retries) and the event is dead-lettered, so giving up never
   * leaves an event falsely marked "counted" — preserving the invariant
   * *dedup-marked ⇒ counted*.
   */
  async forget(eventId: string): Promise<void> {
    await this.redis.del(`${DEDUP_KEY_PREFIX}${eventId}`);
  }
}
