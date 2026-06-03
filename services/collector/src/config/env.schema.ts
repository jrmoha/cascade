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
 * defaults); only the service's own HTTP bind port and the cache TTL carry
 * conventional defaults. Validated once at boot — see {@link AppConfigModule}.
 */
export const collectorEnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3001),
  KAFKA_BOOTSTRAP_SERVERS: csvList,

  // Redis backs the ingest hot-path cache (apiKey→projectId and the per-project
  // event schemas), so the Collector doesn't call Project/Schema per event.
  REDIS_HOST: z.string().min(1),
  REDIS_PORT: z.coerce.number().int().positive(),

  // The Project/Schema gRPC endpoint (KAN-29 contract). Required peer address —
  // the container service name, e.g. `project-schema:50051`.
  PROJECT_SCHEMA_GRPC_URL: z.string().min(1),

  // TTL (seconds) for cached key/schema lookups. Short so revocations and
  // schema edits propagate quickly without a per-event lookup. See ADR-0013.
  PROJECT_SCHEMA_CACHE_TTL_SECONDS: z.coerce.number().int().positive().default(30),
});

export type CollectorConfig = z.infer<typeof collectorEnvSchema>;
