# 0018 — Enforce the CQRS read boundary; counts read endpoint

**Status:** Accepted

## Context

The CQRS read/write separation is the system's single most important constraint
([ADR-0001](0001-overall-architecture.md) §1): analytics is served **only** from the
Aggregator's pre-aggregated read models, never by scanning the raw write path. Later ADRs
sharpened it — [ADR-0008](0008-raw-event-time-range-read.md) drew the real line between
bounded raw **retrieval** (`GET /query`) and **aggregation**, and
[ADR-0015](0015-read-model-aggregation-strategy.md) §1–2 fixed it as "the Query API serves
counters/funnels/retention/boards only from read models; its one raw capability is the
bounded retrieval of ADR-0008."

By the end of KAN-34/35 three of the four analytics surfaces were live and boundary-clean
(`/leaderboard`, `/leaderboard/rank` from Redis; `/funnel`, `/retention` from Postgres).
KAN-36 closes the CQRS story with the parts that were still only _aspirational_:

1. **Counts had a write side but no read side.** KAN-32 landed the `event_counts_by_minute`
   / `event_counts_by_hour` Cassandra `counter` tables but deferred the Query API endpoint
   ("a follow-up ticket"). The AC lists **counts** as a required derived-view read.
2. **The boundary was prose only.** Nothing mechanically stopped an analytics module from
   reading `raw_events`; enforcement lived in CLAUDE.md and code review. The AC asks for a
   "check or documented convention," not hope.
3. **The eventual-consistency lag was undocumented on the read surface.** A freshly
   ingested event appears in aggregates only after the Aggregator processes it; that lag is
   expected, but no reader-facing doc said so.

A subtlety shaped the enforcement design: the counts view lives in **Cassandra** aggregate
tables (ADR-0015 §2), so the boundary is **not** "analytics must not touch Cassandra" — it
is "analytics must not touch the raw **write path** (`raw_events`)" (ADR-0015 §2's
refinement). A naive "no cassandra-driver in analytics" rule would be wrong.

## Decision

### 1. `GET /counts` serves the counter read models, never raw

A new `counts/` module in the Query API serves
`GET /counts?projectId=&from=&to=&granularity=minute|hour&type=` as a **time-series of
per-`(bucket, eventType)` counts** read straight from `event_counts_by_minute` /
`event_counts_by_hour`. It reuses the existing read-only `CassandraService` and the
bucket-walk pattern of the raw retrieval repo (`RawEventReadRepository`): the window maps to
the `(project_id, time_bucket)` partitions it covers and each is read with one prepared,
single-partition `SELECT` — never a cross-partition scan, never `ALLOW FILTERING`. No new
store and no new readiness dep (Cassandra already is one). Response contract is a new Zod
`countsResponse` in `@cascade/contracts` (additive, snapshot-tested — no `schemaVersion`
bump).

### 2. Reads are bounded, so latency is independent of raw-event volume

Per-request fan-out is capped: `hour` reuses `MAX_QUERY_BUCKETS` (168 = 7 days), `minute`
adds `MAX_COUNTS_MINUTE_BUCKETS` (1440 = 24 h); a wider window is a `400`. Each partition
holds at most one row per event type, so a read touches a bounded set of small partitions
whose size is a function of the requested window and the project's event-type cardinality —
**not** of total ingested volume. That is the CQRS payoff made concrete: ingestion can spike
without slowing dashboard reads, because reads hit small pre-computed views, not the
firehose.

### 3. The boundary is enforced by an architecture test

`services/query-api/test/cqrs-boundary.spec.ts` asserts, over the source tree, that the
analytics modules (`counts`, `leaderboard`, `funnel`, `retention`) never reference
`raw_events` or `RawEventReadRepository`, and that `raw_events` appears **only** under
`src/query/` (the sanctioned ADR-0008 retrieval path). It strips comments first, so the docs
that _explain_ the boundary ("served from the aggregate table, never `raw_events`") don't
trip it — it inspects executable code. The test runs in the existing `test:ci` job (no new
tooling). If it ever fails because an analytics module started reading raw rows, the fix is
to **add the missing derived view to the Aggregator, not to relax the check** — that failure
_is_ the "a view is missing" design smell the boundary exists to surface.

### 4. Eventual consistency is documented as expected behaviour

The counts contract, the `/counts` controller, the read-model doc, and the OpenAPI spec all
state that a just-ingested event is counted only after the Aggregator processes it (seconds)
— so a read can lag ingestion. This is by construction (ADR-0015 §1) and must never be
mistaken for a bug.

## Consequences

- New `counts/` module (dto/repository/service/controller) wired into the Query API; new
  `countsResponse` contract and `minuteBucketRange` / `MAX_COUNTS_MINUTE_BUCKETS` helpers in
  `@cascade/contracts`.
- The Query API's four analytics endpoints + `/counts` are now documented as first-class
  read contracts: OpenAPI spec (`docs/specs/query-api.openapi.yaml`), a contracts index row,
  and the runbook — previously only `/query` was specced.
- The CQRS boundary is now a red test, not a convention: a future analytics endpoint that
  reaches for raw rows fails CI. This is deliberately narrow (raw write path only) so
  legitimate Cassandra reads of aggregate tables stay allowed.
- Proven in `query-api/test/counts.repository.spec.ts` (bucket walk, cap, type filter, Long
  conversion), `counts.cassandra.e2e-spec.ts` (real Cassandra, both granularities), and the
  boundary spec. See `docs/read-models/event-counts.md`.

This ADR instantiates [ADR-0015](0015-read-model-aggregation-strategy.md) §2 for the counts
read side and makes [ADR-0001](0001-overall-architecture.md) §1 / [ADR-0008](0008-raw-event-time-range-read.md)
enforceable; it refines neither's policy.
