# 0016 — Idempotent & replayable aggregation (the keystone)

**Status:** Accepted

## Context

[ADR-0015](0015-read-model-aggregation-strategy.md) set the read-model strategy: the
Aggregator derives views from `raw-events`, windows on event time, stays idempotent by
deduping on `eventId`, and keeps every view rebuildable by replaying the log from
offset 0. [ADR-0006](0006-dead-letter-handling.md) established the consumer contract —
delivery is at-least-once, handlers never rethrow, failures route to the DLQ.

KAN-32 then built the first view (per-`(project, type, time-bucket)` event counts) and,
with it, the concrete idempotency machinery: a per-`eventId` Redis dedup gate
(`DedupStore`, `SET aggregator:dedup:{id} 1 NX EX <ttl>`) checked **before** the
non-idempotent counter `+1`, with a compensating `forget()` on give-up.

This ADR is the **keystone** (KAN-33): it does not introduce a new mechanism — it
**pins down and proves** the correctness properties the strategy depends on, so they
are an explicit, tested contract rather than an emergent behaviour. The properties are:
re-delivery is a no-op, replay from offset 0 reproduces aggregates exactly, offsets
commit only after the derived write is durable, and the whole thing is documented with
a known, bounded failure edge. These were partially asserted (a duplicate-counted-once
integration test); the gaps were out-of-order events, replay determinism, and the
offset-commit guarantee — all now closed.

## Decision

### 1. `eventId` is the idempotency key; processed-state lives in Redis, TTL-bounded

Every additive apply is gated on `DedupStore.firstSight(eventId)` — an atomic Redis
`SET … NX EX`. First sight returns `true` (apply the increment); a redelivery within
the horizon returns `false` (skip). The processed-state store is the Redis keyspace
`aggregator:dedup:{eventId}`, and its retention window is `AGGREGATOR_DEDUP_TTL_SECONDS`
— the **lateness horizon**. The horizon is bounded above by the raw-events 30-day TTL
([ADR-0007](0007-cassandra-raw-events-model.md)); a redelivery or replay **older** than
the horizon is no longer deduped and could double-count, which is why correctness for
arbitrarily-old replays is recovered by a full rebuild (§3), not by the gate.

Where a view permits it we still **prefer naturally-idempotent operations** over the
gate (a best-score board via `ZADD GT`, an upsert-by-key summary) — those need no dedup
state. The gate exists for the genuinely additive views (counts), per ADR-0015 §4.

### 2. Offsets commit only after the derived write is durable

The Aggregator consumes through NestJS `ServerKafka`, which runs KafkaJS `eachMessage`.
KafkaJS resolves a message's offset **after** the handler promise resolves. The handler
`await`s the durable counter write before returning, so:

- **Happy path:** the offset is committed only once the write is durable. No lost update.
- **Crash mid-handler:** the offset is never committed; Kafka redelivers the message
  (at-least-once); the dedup gate makes the redelivery a no-op once the original write
  landed, or re-applies it if it did not.
- **Persistent write failure:** after bounded retries the gate is `forget()`-ten and the
  event is dead-lettered; the offset then commits so the partition advances (ADR-0006).
  The event is preserved in the DLQ, not silently dropped.

`autoCommit: true` is set explicitly in `main.ts` (it is also the KafkaJS default) so
this guarantee is visible in code rather than implied.

### 3. Rebuildability: replay from offset 0 reproduces aggregates exactly

Every count is a pure, deterministic function of the log, because it keys off event
time (`occurredAt`) and the stable `eventId`. To rebuild a view: **truncate** its
tables, **flush** the dedup keyspace, and replay `raw-events` from offset 0 with a
**fresh consumer group** (or offsets reset to earliest, `fromBeginning`). A fresh dedup
state is essential — the replay re-presents every event including its in-log
duplicates, and the gate must dedup the replay against itself, not against the
already-expired markers of the original pass. The result is identical to a single clean
pass.

### 4. The honest target is effectively-once, with one bounded residual edge

True cross-system exactly-once is a myth here: the sinks (Cassandra, Redis) are external
to Kafka, so Kafka EOS would not cover them. At-least-once delivery + idempotent
processing gives an **effectively-once outcome**, which is the target.

One residual edge is accepted and documented: if the process crashes **between** the
`SETNX` dedup mark and the counter write, the offset is uncommitted, the redelivery is
skipped by the now-set marker, and that single increment is lost (a rare under-count).
This is the inherent cost of coordinating two non-transactional stores; it is bounded to
at most one increment per crash and is **healed by a rebuild** (§3). We keep the simple
mark-then-write ordering rather than redesigning the counter into a contributing-set
model, because the rebuild backstop makes the trade worthwhile.

## Consequences

- The four properties are now an **executable contract**:
  [`services/aggregator/test/idempotency.e2e-spec.ts`](../../services/aggregator/test/idempotency.e2e-spec.ts)
  proves out-of-order distinct events bucket by event time, a replay from offset 0
  reproduces the full aggregate state byte-for-byte, and redelivery across a consumer
  restart neither double-counts nor loses updates;
  [`test/event-counts.e2e-spec.ts`](../../services/aggregator/test/event-counts.e2e-spec.ts)
  continues to prove a redelivered `eventId` is counted once.
- The dedup TTL is a **correctness knob**, not just a memory bound: it must be ≥ the
  largest lateness/redelivery gap the deployment expects, and ≤ the raw-events TTL.
- Future additive views (e.g. total-score boards) inherit this contract by reusing
  `DedupStore`; naturally-idempotent views may skip the gate entirely.
- Operators get a documented rebuild runbook shape (truncate → flush dedup → replay
  from a fresh group); a botched or newly-added view is "design + replay", not a
  bespoke backfill.

This ADR refines, and does not supersede, [ADR-0015](0015-read-model-aggregation-strategy.md)
§4–§5; it is the proof-and-precision layer over that strategy.
