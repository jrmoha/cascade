# 0008 — Bounded raw event-retrieval read (time-range `GET /query`)

**Status:** Accepted (supersedes [0003](0003-query-api-phase0-raw-read.md))

## Context

ADR-0001 draws a hard line: the Query API serves analytics from pre-aggregated **read models**
(maintained by the Aggregator), and there is deliberately no arrow from the Query API to Cassandra.
ADR-0003 then carved a **Phase-0-only** exception — a throwaway `GET /query?projectId=&hours=` that
reads raw `cascade.raw_events` directly — purely to close the walking-skeleton loop, and declared
it would be _deleted_ in Phase 1.

KAN-25 ("Time-range read API against the raw model") asks for something that looks similar but is
materially different in intent: a **proper, supported** time-range read of a project's raw events —
`GET /query?projectId=&from=&to=`, time-ordered and paginated — built against the query-first model
from KAN-24/ADR-0007. The acceptance criteria explicitly forbid full scans and `ALLOW FILTERING`.

This forces a decision the team flagged before building: does "the Query API never reads raw
Cassandra" mean _never, for anything_, or _never for analytics_? Deleting the raw read (ADR-0003's
stated plan) would make KAN-25 unbuildable. Keeping it as-is leaves a "temporary" hack as load-
bearing. Neither is honest.

## Decision

Promote a **bounded raw event-retrieval read** to a first-class, supported capability, and draw the
real architectural boundary where it belongs — between **retrieval** and **aggregation**, not
between "Query API" and "Cassandra":

1. **Retrieval reads raw; analytics reads models.** `GET /query` returns raw `RawEvent` envelopes
   for replay / audit / debugging. Counters, funnels, retention and leaderboards are **aggregation**
   and continue to be served **exclusively** from Aggregator read models (Redis / Postgres) — those
   never touch raw Cassandra. The non-negotiable rule in CLAUDE.md is re-read with this scope: _the
   Query API never **scans** raw Cassandra for analytics_.

2. **Always partition-key-bounded — never a scan.** `[from, to]` is mapped to the hourly
   `time_bucket` partitions it covers (`hourlyBucketRange` in `@cascade/contracts`); each is read
   with one prepared, `occurred_at`-bounded single-partition `SELECT`. No cross-partition scan, no
   `ALLOW FILTERING`. A window may span at most `MAX_QUERY_BUCKETS = 168` buckets (7 days); wider
   windows are rejected with `400`. This keeps the fan-out of any single request bounded.

3. **Newest-first with no app-side sort.** The table's `CLUSTERING ORDER BY (occurred_at DESC, …)`
   plus a newest-first bucket walk yields a globally ordered result by concatenation (ADR-0007).

4. **Cursor pagination over native paging-state.** Paging uses the cassandra-driver's per-partition
   paging-state, carried between calls in an opaque base64url cursor that also pins the bucket it
   belongs to. Default page size 100, max 1000. An **absent** `nextCursor` is the only guaranteed
   end-of-window signal; a present one may still yield an empty next page. Native paging-state was
   chosen over keyset `(occurred_at, event_id)` slices because the clustering order is _mixed_
   (`occurred_at DESC, event_id ASC`), which makes multi-column keyset predicates error-prone; the
   driver resumes a partition correctly regardless.

Alternatives rejected:

- **Delete the raw read (ADR-0003's original plan), serve everything from read models.** Rejected:
  raw event retrieval (replay/audit/"show me this project's last hour of events") is a genuine,
  ongoing need that an aggregation read model does not answer. Forcing it through aggregates would
  either lose fidelity or rebuild raw storage behind a different door.
- **Keyset pagination on `(occurred_at, event_id)`.** Rejected for now: brittle under the mixed
  clustering order. Reconsider if we ever need cursors that survive schema/driver changes.
- **`ALLOW FILTERING` / `projectId`-only (no window).** Rejected: the exact anti-pattern ADR-0003
  and the Cassandra rules forbid.

## Consequences

- **ADR-0003 is superseded.** The "delete in Phase 1" plan is withdrawn; the raw read stays, but
  re-scoped and hardened (real time-range, pagination, span cap) rather than a walking-skeleton
  hack. The Phase-0 `hours=` parameter is replaced by `from`/`to`; the smoke test moves to the new
  shape.
- **The read/write separation rule is sharpened, not weakened.** "Never scan raw for analytics"
  remains absolute. A reviewer seeing a Query API feature that needs to _aggregate_ over raw rows
  should still stop and flag it — this ADR sanctions **bounded retrieval only**.
- **Still no aggregation semantics here.** `GET /query` returns envelopes, not metrics. Counters /
  funnels / leaderboards remain Aggregator/read-model work.
- **The Query API keeps its Cassandra dependency** (read-only; it performs no DDL — the
  Ingestion-Processor still owns the schema).
- **Bounded cost.** `MAX_QUERY_BUCKETS` caps the partitions any one request can fan out to; `limit`
  caps rows per page. A sparse window can still cost several empty single-partition reads to fill a
  page — acceptable, and bounded by the span cap.

Documented in [`docs/contracts/events.md`](../contracts/events.md) → "Reading events back".
