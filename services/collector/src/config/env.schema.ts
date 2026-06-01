import { z } from 'zod';

/**
 * Parses a required, comma-separated env var (e.g. `host1:9092,host2:9092`)
 * into a non-empty string array. No default — a missing/empty value fails
 * validation at boot rather than silently falling back to localhost.
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
 * Collector environment contract. Infra/peer addresses are required (no
 * defaults); only the service's own HTTP bind port carries a conventional
 * default. Validated once at boot — see {@link AppConfigModule}.
 */
export const collectorEnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3001),
  KAFKA_BOOTSTRAP_SERVERS: csvList,
});

export type CollectorConfig = z.infer<typeof collectorEnvSchema>;
