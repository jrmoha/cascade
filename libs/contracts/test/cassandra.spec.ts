import { describe, expect, it } from 'vitest';
import { types } from 'cassandra-driver';
import {
  cassandraConsistencySchema,
  toDriverConsistency,
  type CassandraConsistency,
} from '../src/cassandra';

/**
 * The consistency contract hardcodes the CQL native-protocol codes so the shared
 * lib stays driver-free (KAN-38 / ADR-0019). This test is the tripwire that keeps
 * those codes honest: it asserts every mapped value equals cassandra-driver's own
 * `types.consistencies` constant. If the driver ever renumbers (it won't — they're
 * wire-protocol constants), this fails instead of silently reading at the wrong
 * level.
 */
describe('cassandra consistency contract', () => {
  const expected: Record<CassandraConsistency, number> = {
    any: types.consistencies.any,
    one: types.consistencies.one,
    two: types.consistencies.two,
    three: types.consistencies.three,
    quorum: types.consistencies.quorum,
    all: types.consistencies.all,
    local_quorum: types.consistencies.localQuorum,
    each_quorum: types.consistencies.eachQuorum,
    local_one: types.consistencies.localOne,
  };

  it.each(cassandraConsistencySchema.options)('maps %s to the driver constant', (level) => {
    expect(toDriverConsistency(level)).toBe(expected[level]);
  });

  it('rejects an unknown level', () => {
    expect(cassandraConsistencySchema.safeParse('super_quorum').success).toBe(false);
  });
});
