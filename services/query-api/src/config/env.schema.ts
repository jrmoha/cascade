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
 * Query API environment contract. Peer addresses are required (no defaults);
 * only the service's own HTTP bind port carries a conventional default. Validated
 * once at boot — see {@link AppConfigModule}.
 *
 * The Query API reads from three stores: Cassandra (bounded raw retrieval,
 * ADR-0008), Redis (the leaderboard read model, ADR-0015 / KAN-34), and Postgres
 * (the funnel & retention summaries, ADR-0017 / KAN-35).
 */
export const queryApiEnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3002),
  CASSANDRA_CONTACT_POINTS: csvList,
  CASSANDRA_PORT: z.coerce.number().int().positive(),
  CASSANDRA_LOCAL_DC: z.string().min(1),
  /**
   * Read consistency level set explicitly on the driver client (KAN-38,
   * ADR-0019): `local_quorum` in the cluster. Required (no default — never the
   * driver default `LOCAL_ONE`). The Query API is read-only and creates no
   * keyspace, so it takes no replication factor.
   */
  CASSANDRA_CONSISTENCY: cassandraConsistencySchema,
  REDIS_HOST: z.string().min(1),
  REDIS_PORT: z.coerce.number().int().positive(),
  DATABASE_URL: z.string().min(1),
  /**
   * Optional Postgres **read-replica** URL (KAN-41, ADR-0019 §2). The funnel and
   * retention analytics reads are eventually-consistent by construction, so they
   * are served from a streaming replica that trails the primary by a bounded lag.
   * When unset (single-node dev/test), reads fall back to {@link DATABASE_URL} —
   * so the Testcontainers suites and the smoke test run unchanged.
   */
  DATABASE_REPLICA_URL: z.string().min(1).optional(),
});

export type QueryApiConfig = z.infer<typeof queryApiEnvSchema>;
