import { describe, expect, it } from 'vitest';
import { aggregatorEnvSchema } from '../src/config/env.schema';

const base = {
  KAFKA_BOOTSTRAP_SERVERS: 'localhost:9092',
  CASSANDRA_CONTACT_POINTS: 'localhost',
  CASSANDRA_PORT: '9042',
  CASSANDRA_LOCAL_DC: 'datacenter1',
  CASSANDRA_REPLICATION_FACTOR: '1',
  CASSANDRA_CONSISTENCY: 'local_quorum',
  REDIS_HOST: 'localhost',
  REDIS_PORT: '6379',
  DATABASE_URL: 'postgresql://cascade:cascade@localhost:5432/cascade',
  AGGREGATOR_DEDUP_TTL_SECONDS: '86400',
  AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS: '172800',
};

describe('aggregatorEnvSchema', () => {
  it('parses a complete env, coercing numbers and splitting CSV lists', () => {
    const cfg = aggregatorEnvSchema.parse(base);
    expect(cfg.PORT).toBe(3005); // default
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(['localhost:9092']);
    expect(cfg.CASSANDRA_CONTACT_POINTS).toEqual(['localhost']);
    expect(cfg.CASSANDRA_PORT).toBe(9042);
    expect(cfg.CASSANDRA_REPLICATION_FACTOR).toBe(1);
    expect(cfg.CASSANDRA_CONSISTENCY).toBe('local_quorum');
    expect(cfg.REDIS_PORT).toBe(6379);
    expect(cfg.AGGREGATOR_DEDUP_TTL_SECONDS).toBe(86400);
    expect(cfg.AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS).toBe(172800);
  });

  it('splits a comma-separated broker/contact-point list and trims blanks', () => {
    const cfg = aggregatorEnvSchema.parse({
      ...base,
      KAFKA_BOOTSTRAP_SERVERS: 'a:9092, b:9092 ,',
    });
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(['a:9092', 'b:9092']);
  });

  it('fails fast when a required infra var is missing (no silent localhost default)', () => {
    const withoutDb: Partial<typeof base> = { ...base };
    delete withoutDb.DATABASE_URL;
    expect(() => aggregatorEnvSchema.parse(withoutDb)).toThrow();
  });

  it('rejects a non-URL DATABASE_URL', () => {
    expect(() => aggregatorEnvSchema.parse({ ...base, DATABASE_URL: 'not-a-url' })).toThrow();
  });

  it('requires AGGREGATOR_DEDUP_TTL_SECONDS to be a positive integer', () => {
    const withoutTtl: Partial<typeof base> = { ...base };
    delete withoutTtl.AGGREGATOR_DEDUP_TTL_SECONDS;
    expect(() => aggregatorEnvSchema.parse(withoutTtl)).toThrow();
    expect(() =>
      aggregatorEnvSchema.parse({ ...base, AGGREGATOR_DEDUP_TTL_SECONDS: '0' }),
    ).toThrow();
    expect(() =>
      aggregatorEnvSchema.parse({ ...base, AGGREGATOR_DEDUP_TTL_SECONDS: '-5' }),
    ).toThrow();
  });
});
