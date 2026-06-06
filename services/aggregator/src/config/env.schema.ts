import { z } from 'zod';

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
});

export type CassandraEnv = z.infer<typeof cassandraEnvSchema>;
export type AggregatorConfig = z.infer<typeof aggregatorEnvSchema>;
