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

  // --- Ingestion resilience (KAN-42, ADR-0021) ---
  // These are tuning knobs, not peer addresses, so they carry conventional
  // defaults (unlike infra addresses, which must be supplied).

  // Per-API-key Redis token bucket: sustained refill rate (tokens/sec) and the
  // burst capacity (bucket size). A key over its budget gets a 429.
  RATE_LIMIT_REFILL_PER_SEC: z.coerce.number().positive().default(50),
  RATE_LIMIT_BURST: z.coerce.number().int().positive().default(100),

  // Backpressure: the hard cap on Kafka produces in flight at once. Beyond it
  // the Collector returns 503 rather than buffering unboundedly (never drops).
  PRODUCE_MAX_INFLIGHT: z.coerce.number().int().positive().default(500),
  // Bound on a single produce attempt; a stuck broker fails fast into retry/503.
  PRODUCE_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  // Bounded exponential backoff on a transient produce failure: waits are
  // PRODUCE_RETRY_BASE_MS * 2^(n-1); exhaustion returns 503 (client retries).
  PRODUCE_MAX_ATTEMPTS: z.coerce.number().int().positive().default(3),
  PRODUCE_RETRY_BASE_MS: z.coerce.number().int().positive().default(100),

  // Circuit breaker around the Project/Schema gRPC calls (opossum). Trip when
  // the error rate exceeds ERROR_PCT over at least VOLUME calls; stay open for
  // RESET_MS before a trial call. NOT_FOUND (unregistered schema) never trips it.
  PROJECT_SCHEMA_BREAKER_ERROR_PCT: z.coerce.number().int().min(1).max(100).default(50),
  PROJECT_SCHEMA_BREAKER_RESET_MS: z.coerce.number().int().positive().default(10000),
  PROJECT_SCHEMA_BREAKER_VOLUME: z.coerce.number().int().positive().default(5),
});

export type CollectorConfig = z.infer<typeof collectorEnvSchema>;
