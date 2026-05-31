# 0006 — Dead-letter handling for failed events

**Status:** Accepted

## Context

Edge validation (KAN-22) rejects malformed input synchronously at the Collector. But an event that
passed the edge can still fail **downstream** in the Ingestion-Processor: the consumer may receive a
message it can't deserialize/validate (e.g. a non-JSON value, or a producer that bypassed the
Collector), or Cassandra may reject/refuse the write. ADR-0001 fixes delivery as **at-least-once**
with idempotent consumers.

Two failures must be avoided:

1. **Silent loss** — a failed event simply disappearing.
2. **Head-of-line blocking** — one "poison" message stalling its entire partition, because a thrown
   handler under at-least-once delivery is redelivered forever.

## Decision

Route failed messages to a **dead-letter topic** with enough context to inspect and replay them, and
classify failures so we only retry what's worth retrying.

1. **DLQ topic `raw-events.dlq`.** The dead-letter envelope is a shared Zod contract
   (`deadLetterSchema` / `DeadLetter` in `@cascade/contracts`): `originalValue` (the raw message,
   verbatim, so even un-parseable messages replay losslessly), `originalEvent` (the parsed event,
   when it was valid), `error { kind, reason }`, `attempts`, `failedAt`, and `source { topic,
partition, offset, key }`.

2. **Failure taxonomy.**

   - **`validation`** — the message fails `rawEventSchema` (deserialization/contract). **Permanent**:
     retrying can't help, so it is dead-lettered immediately (`attempts: 1`).
   - **`persistence`** — a valid event whose Cassandra write throws. **Transient**: retried before
     dead-lettering.

3. **Bounded retry for transient failures: `MAX_ATTEMPTS = 3`, exponential backoff `200ms` then
   `400ms`** (`RETRY_BASE_MS * 2^(n-1)`), in-process and synchronous on the consumer. If all attempts
   fail, the event is dead-lettered with `attempts: 3` and `kind: 'persistence'`.

4. **The handler never rethrows.** On any outcome (success, dead-letter) it returns normally so the
   offset commits and the partition advances — a poison message can neither crash the consumer nor
   block subsequent messages.

5. **The Processor gets a Kafka producer** (`ClientKafka`, producer-only, `DefaultPartitioner` — same
   as the Collector, ADR-0002) used solely to publish to the DLQ, keyed by the original `projectId`.

Alternatives considered:

- **Throw and rely on Kafka redelivery:** poison messages loop forever and block the partition —
  rejected.
- **A separate retry topic / delayed-retry ladder:** more robust for high-volume transient outages,
  but heavier than Phase 1 needs. The in-process bounded retry is a deliberate, documented
  simplification; revisit under load.
- **Skip-and-log (the KAN-21 stopgap):** loses the event with no replay path — replaced by this.

## Consequences

- No silent loss and no head-of-line blocking; failed events are inspectable and replayable from
  `raw-events.dlq` (see `docs/runbooks/dlq.md`).
- In-process retries are synchronous, so a sustained Cassandra outage slows that partition for up to
  ~600ms per message before dead-lettering. Acceptable at Phase 1 volume; the retry topic is the
  escalation path.
- The DLQ is **not** auto-drained — replay is a manual/operational step for now (documented in the
  runbook). Automated replay is a future ticket.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` creates `raw-events.dlq` on first publish locally; provision it
  explicitly in real environments.
