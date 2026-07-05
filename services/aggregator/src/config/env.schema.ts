import { z } from 'zod';
import { cassandraConsistencySchema } from '@cascade/contracts';

/**
 * Parses a required, comma-separated env var into a non-empty string array.
 * No default — a missing/empty value fails validation at boot rather than
 * silently falling back to localhost.
 */
const csvList = z
  .string()
  .transform((s) =>
    s
      .split(',')
      .map((part) => part.trim())
      .filter(Boolean),
  )
  .refine((list) => list.length > 0, 'must contain at least one entry');

/**
 * Cassandra connection contract. Split out so the standalone migration
 * entrypoint (`migrate.ts`) can validate just these vars without requiring the
 * Kafka/Redis/Postgres ones it doesn't use.
 */
export const cassandraEnvSchema = z.object({
  CASSANDRA_CONTACT_POINTS: csvList,
  CASSANDRA_PORT: z.coerce.number().int().positive(),
  CASSANDRA_LOCAL_DC: z.string().min(1),
  /**
   * Keyspace replication factor for `NetworkTopologyStrategy` (KAN-38, ADR-0019).
   * The migrator creates `cascade` with `{<CASSANDRA_LOCAL_DC>: <RF>}`. Required
   * (no default) — a wrong RF is a silent durability footgun; the 3-node cluster
   * uses 3, single-node dev/test uses 1. RF must be ≤ node count.
   */
  CASSANDRA_REPLICATION_FACTOR: z.coerce.number().int().positive(),
  /**
   * Read/write consistency level set explicitly on the driver client (ADR-0019):
   * `local_quorum` in the cluster (`R+W>RF` ⇒ strong per-DC). Required (no default,
   * never the driver default `LOCAL_ONE`). The demo flips it to feel ONE vs QUORUM.
   */
  CASSANDRA_CONSISTENCY: cassandraConsistencySchema,
});

/**
 * Aggregator environment contract. Infra/peer addresses are required (no
 * defaults); only the service's own HTTP bind port (for health probes — the
 * Aggregator is a hybrid HTTP + Kafka-consumer app) carries a conventional
 * default. Validated once at boot — see {@link AppConfigModule}.
 *
 * The Aggregator writes read models to three stores (ADR-0015): Cassandra
 * (counters), Redis (leaderboards + dedup), Postgres (funnel/retention).
 */
export const aggregatorEnvSchema = cassandraEnvSchema.extend({
  PORT: z.coerce.number().int().positive().default(3005),
  KAFKA_BOOTSTRAP_SERVERS: csvList,
  REDIS_HOST: z.string().min(1),
  REDIS_PORT: z.coerce.number().int().positive(),
  DATABASE_URL: z.string().url(),
  /**
   * Lateness horizon for the per-`eventId` dedup guard that keeps the additive
   * event counters replay-safe (ADR-0015 §4): a redelivery seen within this TTL
   * is a no-op. Required (no inline default) — it is a correctness/retention
   * knob, bounded above by the raw-events 30-day TTL (ADR-0007). See
   * {@link DedupStore}.
   */
  AGGREGATOR_DEDUP_TTL_SECONDS: z.coerce.number().int().positive(),
  /**
   * Retention for the **daily** leaderboard sorted sets (KAN-34, ADR-0015 §2):
   * each per-UTC-day board (`lb:{projectId}:{YYYY-MM-DD}`) is given/refreshed this
   * TTL on write so old days self-expire, while the all-time board never expires.
   * Required (no inline default); a retention knob, bounded above by the
   * raw-events 30-day TTL (ADR-0007). See {@link LeaderboardRepository}.
   */
  AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS: z.coerce.number().int().positive(),
});

export type CassandraEnv = z.infer<typeof cassandraEnvSchema>;
export type AggregatorConfig = z.infer<typeof aggregatorEnvSchema>;
