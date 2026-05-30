# 0003 — Query API Phase-0 raw read-back from Cassandra

**Status:** Accepted (temporary — to be superseded in Phase 1)

## Context

KAN-19 adds the Query API with a single `GET /query?projectId=...` endpoint. Its
purpose is narrow: close the Phase 0 walking-skeleton loop
(Collector → Kafka → Ingestion-Processor → Cassandra → **read back**) so the
KAN-20 end-to-end smoke test has something to assert against.

This collides head-on with a non-negotiable rule from ADR-0001 and the C4
container view: **the Query API must never read from Cassandra.** The target
design serves every query from pre-aggregated read models (Redis / PostgreSQL)
that the Aggregator maintains. There is deliberately no arrow from the Query API
to Cassandra.

But in Phase 0 the Aggregator and its read models do not exist yet. The only
place the ingested data lives is `cascade.raw_events`. To prove the pipe end to
end without building the entire read path first, the Query API needs to read
that table directly — once, temporarily.

## Decision

For **Phase 0 only**, the Query API reads raw events directly from Cassandra,
under tight constraints that keep the shortcut honest:

1. **Partition-key-bounded reads only — never `ALLOW FILTERING`.** The endpoint
   takes `projectId` and an hourly-bucket lookback `hours` (default 1 = current
   hour). It issues one prepared single-partition `SELECT ... WHERE project_id = ?
AND time_window = ?` per bucket and merges the results. It never scans across
   partitions. This respects the query-first Cassandra rule even while breaking
   the "no Cassandra reads" rule.

2. **Read-only.** The Query API's `CassandraService` performs no DDL; the
   Ingestion-Processor remains the sole owner of the schema and the sole writer.

3. **Clearly fenced as temporary.** The controller, repository, runbook, and this
   ADR all flag the raw read-back as a walking-skeleton shortcut slated for
   removal. The hourly-bucket helpers (`toHourlyWindow`, `recentHourlyWindows`)
   live in `@cascade/contracts` so the write and read paths compute identical
   partition keys.

Alternatives rejected:

- **Build the Aggregator + read models first (do it "right" now).** Rejected: it
  front-loads the most complex part of the system before the skeleton is proven,
  defeating the point of Phase 0.
- **`ALLOW FILTERING` to support `projectId`-only with no window.** Rejected: a
  cross-partition scan is the exact anti-pattern the Cassandra rules forbid, and
  it would set a bad precedent even temporarily.

## Consequences

- **This is a known, documented violation of the read/write separation**, scoped
  to Phase 0 and tracked for removal. It must not be cited as precedent for
  Query API features reading raw Cassandra.
- **Phase 1 supersedes this.** When the Aggregator and read models land, the
  Query API switches to reading pre-aggregates; the raw-read repository and the
  Query API's Cassandra dependency are deleted. This ADR moves to _Superseded_.
- **Bounded, not unlimited.** `hours` is capped (≤ 168) so a request can only
  ever fan out to a bounded number of single-partition reads.
- **No aggregation semantics yet.** `GET /query` returns raw event envelopes, not
  counters/funnels/leaderboards. Those are read-model features for Phase 1.
