# Read model — event counts (windowed)

The Aggregator's first derived view (KAN-32): near-real-time **event counts** per
`(project, eventType, time-bucket)`, so a project owner sees activity as it happens
without ever scanning raw events. This is the canonical worked example of the
[ADR-0015](../adr/0015-read-model-aggregation-strategy.md) read-model strategy:
event-time windowing, dedup-by-`eventId` idempotency, and replay-based rebuild.

> **Strategy vs. implementation.** The _why_ lives in
> [ADR-0015](../adr/0015-read-model-aggregation-strategy.md); this page is the
> _what_ for the counts view specifically.

## What it serves

Time-series counts for a `(projectId, type)` over a time range, at **minute** and
**hour** granularity — e.g. "how many `level_complete` events did `game-1` see per
minute over the last hour". Both granularities are stored directly, so a read at
either is O(1) per bucket. The Query API endpoint that exposes these is a
follow-up ticket; KAN-32 lands the write side and its tables.

## Store & schema (Cassandra)

Counts are a high-write, append-mostly, range-by-bucket access pattern → Cassandra
`counter` aggregate tables (ADR-0015 §2). These are the Aggregator's **own** tables;
they are never the raw write path (`raw_events`) and the Ingestion-Processor never
touches them. DDL is a versioned migration applied by the Aggregator's `Migrator`
on boot:
[`services/aggregator/migrations/cassandra/0001_create_event_counts.cql`](../../services/aggregator/migrations/cassandra/0001_create_event_counts.cql).

```sql
CREATE TABLE cascade.event_counts_by_minute (
  project_id  text,
  time_bucket text,        -- 'YYYY-MM-DDTHH:MM' minute bucket of occurredAt (UTC)
  event_type  text,
  count       counter,
  PRIMARY KEY ((project_id, time_bucket), event_type)
);
-- event_counts_by_hour is identical with a 'YYYY-MM-DDTHH' hour bucket.
```

Partition discipline mirrors `raw_events` ([ADR-0007](../adr/0007-cassandra-raw-events-model.md)):
the partition key `((project_id, time_bucket))` bounds partition size to one project

- one time-unit, and clustering on `event_type` lets a `(project, type)`-over-range
  read enumerate buckets with single-partition `SELECT`s — never a cross-partition
  scan, never `ALLOW FILTERING`.

## Windowing — event time

Both buckets are derived from the event's **`occurredAt`** (event time), not arrival
time, via the shared helpers in
[`libs/contracts/src/time-window.ts`](../../libs/contracts/src/time-window.ts):
`toMinuteBucket` (added for this view) and `toHourlyBucket`. A late or out-of-order
event therefore lands in the minute/hour for **when it happened** — the whole reason
[ADR-0004](../adr/0004-canonical-event-contract.md) split event time from ingest time.

## Idempotency — dedup by `eventId`

Kafka is at-least-once and a `counter` `+1` is **not** replay-safe (applied twice =
double-count), which the "never double-count" rule forbids. A `counter` column also
can't be part of the primary key, so the row key can't enforce idempotency the way
`raw_events` does. Instead the consumer gates every increment on a per-`eventId`
**dedup guard** (ADR-0015 §4):

1. `DedupStore.firstSight(eventId)` → Redis `SET aggregator:dedup:{eventId} 1 NX EX <ttl>`.
   First sight returns `true`; a redelivery within the horizon returns `false` and the
   event is skipped (no-op).
2. On first sight, increment the minute **and** hour counters (bounded retry on
   transient Cassandra failure, like the Ingestion-Processor).
3. If the write ultimately fails, `DedupStore.forget(eventId)` clears the marker
   (so the uncounted event can be re-counted later) and the event is dead-lettered
   to `raw-events.dlq`. The handler never rethrows.

The TTL is the **lateness horizon**, configured by `AGGREGATOR_DEDUP_TTL_SECONDS`
(required env), bounded above by the raw-events 30-day TTL.

**Offset commit (no lost updates).** The consumer commits a message's offset only
**after** the durable counter write — NestJS `ServerKafka` runs KafkaJS `eachMessage`,
which resolves the offset after the handler returns, and the handler `await`s the write
first ([ADR-0016](../adr/0016-idempotent-replayable-aggregation.md) §2). A crash
mid-handler leaves the offset uncommitted, so Kafka redelivers (at-least-once) rather
than losing the event.

**Residual edge:** if the process crashes between the `SETNX` and the counter write,
the offset isn't committed, the redelivery is skipped by the now-set dedup key, and
that one increment is lost (a rare under-count). This is the inherent at-least-once
edge ADR-0015 accepts; it — and any transient minute/hour divergence — is healed by
a rebuild.

## Rebuild

Every count is a pure, deterministic function of the log. To rebuild: truncate
`event_counts_by_minute` / `event_counts_by_hour` (and flush the dedup keyspace),
then replay `raw-events` from offset 0 with the Aggregator (a fresh consumer group
or offsets reset to earliest). Flushing the dedup keyspace is essential — the replay
re-presents every event including its in-log duplicates, and the gate must dedup the
replay against itself, not against the original pass's expired markers. Determinism
holds because counts key off event time and the stable `eventId`
([ADR-0015](../adr/0015-read-model-aggregation-strategy.md) §5,
[ADR-0016](../adr/0016-idempotent-replayable-aggregation.md) §3).

## Tests

- **Unit** — [`test/aggregator.controller.spec.ts`](../../services/aggregator/test/aggregator.controller.spec.ts):
  first-sight → increment; duplicate → skip; write failure → retry then `forget` +
  DLQ; handler never throws.
- **Integration** — [`test/event-counts.e2e-spec.ts`](../../services/aggregator/test/event-counts.e2e-spec.ts):
  real Kafka + Cassandra + Redis; feeds a known event set (including a redelivered
  `eventId`) and asserts the minute and hour counters match **exactly**, with the
  duplicate counted once.
- **Integration (keystone)** — [`test/idempotency.e2e-spec.ts`](../../services/aggregator/test/idempotency.e2e-spec.ts):
  the idempotency/replayability contract of
  [ADR-0016](../adr/0016-idempotent-replayable-aggregation.md). Proves (a) out-of-order
  **distinct** events bucket by event time regardless of arrival order; (b) a replay
  from offset 0 into truncated tables with a flushed dedup keyspace reproduces the full
  aggregate state **byte-for-byte**; (c) redelivery across a consumer restart neither
  double-counts nor loses updates (dedup state survives the restart).
