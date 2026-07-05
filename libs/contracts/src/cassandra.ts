import { z } from 'zod';

/**
 * Cassandra consistency-level contract (KAN-38, ADR-0019). The replication &
 * consistency model fixes reads/writes at `LOCAL_QUORUM`, but the level is a
 * per-service **env knob** (`CASSANDRA_CONSISTENCY`) so it can be tuned and so the
 * ONE-vs-QUORUM tradeoff can be demonstrated. This is the single source of truth
 * for the accepted level names and their mapping to the driver's consistency
 * codes, shared by every service that owns a Cassandra client — so the three
 * clients can never disagree on the spelling or the mapping.
 *
 * `local_*` levels are quorum/one **within the local datacenter** (the DC named
 * by `CASSANDRA_LOCAL_DC`), which is why ADR-0019 picks `local_quorum` even
 * single-region: multi-region later is config, not a rewrite.
 */
export const cassandraConsistencySchema = z.enum([
  'any',
  'one',
  'two',
  'three',
  'quorum',
  'all',
  'local_quorum',
  'each_quorum',
  'local_one',
]);
export type CassandraConsistency = z.infer<typeof cassandraConsistencySchema>;

/**
 * The CQL native-protocol consistency codes. These are fixed wire-protocol
 * constants (they equal cassandra-driver's `types.consistencies` values), so we
 * map to them here rather than importing cassandra-driver — keeping this shared
 * contract dependency-free (Collector / Project-Schema don't use Cassandra and
 * must not pull the driver in). The number is passed straight to the driver's
 * `queryOptions.consistency` (or a per-statement override).
 */
const CONSISTENCY_CODE: Record<CassandraConsistency, number> = {
  any: 0,
  one: 1,
  two: 2,
  three: 3,
  quorum: 4,
  all: 5,
  local_quorum: 6,
  each_quorum: 7,
  local_one: 10,
};

/** Maps a validated `CASSANDRA_CONSISTENCY` value to its cassandra-driver
 * consistency code, for use as a client's default `consistency`. */
export function toDriverConsistency(level: CassandraConsistency): number {
  return CONSISTENCY_CODE[level];
}
