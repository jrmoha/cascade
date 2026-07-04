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
  REDIS_HOST: z.string().min(1),
  REDIS_PORT: z.coerce.number().int().positive(),
  DATABASE_URL: z.string().min(1),
});

export type QueryApiConfig = z.infer<typeof queryApiEnvSchema>;
