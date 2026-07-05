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
 * Kafka ones it doesn't use.
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
 * Ingestion-Processor environment contract. Infra/peer addresses are required
 * (no defaults); only the service's own HTTP bind port (for health probes —
 * the processor is a hybrid HTTP + Kafka-consumer app) carries a conventional
 * default. Validated once at boot — see {@link AppConfigModule}.
 */
export const ingestionEnvSchema = cassandraEnvSchema.extend({
  PORT: z.coerce.number().int().positive().default(3003),
  KAFKA_BOOTSTRAP_SERVERS: csvList,
});

export type CassandraEnv = z.infer<typeof cassandraEnvSchema>;
export type IngestionConfig = z.infer<typeof ingestionEnvSchema>;
