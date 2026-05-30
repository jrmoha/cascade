# 0002 — Collector Kafka production strategy

**Status:** Accepted

## Context

KAN-17 introduces the Collector, the head of the write path. It accepts events over HTTP
and produces them to the Kafka `raw-events` topic. Three concrete choices had to be made
that ADR-0001 left open:

1. **Which Kafka client** the NestJS service uses to produce.
2. **What the Kafka message key is** — this determines partition placement and ordering.
3. **How `eventId` is assigned**, given ADR-0001's at-least-once delivery and
   dedup-by-event-id idempotency requirement for downstream consumers.

ADR-0001 already fixed the surrounding constraints: at-least-once delivery, idempotent
consumers, and Cassandra partitioning by `(project_id, time_window)`.

## Decision

1. **Producer client: `@nestjs/microservices` Kafka transport (`ClientKafka`)**, in
   producer-only mode, over `kafkajs`. Brokers come from `KAFKA_BOOTSTRAP_SERVERS`.
   Chosen for idiomatic NestJS integration and a single transport abstraction reused by
   the downstream consumer services (KAN-18/19). The alternative — using `kafkajs`
   directly in a hand-rolled provider — was rejected to avoid two parallel Kafka
   integration styles in the monorepo.

2. **Message key = `projectId`**, with the producer pinned to KafkaJS's
   `DefaultPartitioner` (Java-client-compatible murmur2 hash). All events for a project
   are routed to the same partition, which preserves per-project ordering and aligns with
   the Cassandra `(project_id, time_window)` partitioning so a project's stream stays
   coherent through the pipeline. Pinning the partitioner explicitly also keeps placement
   stable if other Kafka clients (e.g. tooling, future non-Node producers) write to the
   same topic, and silences KafkaJS's v2 partitioner-change warning.

3. **`eventId` is server-stamped (UUID v4) by the Collector** when the client does not
   supply one. This guarantees every event carries a stable, unique idempotency key
   before it hits Kafka — the key downstream consumers dedup on (Cassandra clustering
   key), satisfying the at-least-once + idempotency rule from ADR-0001.

## Consequences

- **Ordering is per-project, not global.** Acceptable: aggregations are scoped per
  project. A very hot `projectId` becomes a partition hotspot; revisit with a composite
  key (e.g. `projectId + bucket`) under load testing in Phase 2 if needed.
- **Client-supplied `eventId` is not yet honored** (Phase 0 always generates one). When
  clients need idempotent retries end-to-end, accepting a client `eventId` is a small
  Phase 1 follow-up; the envelope already carries the field.
- **Coupling to the NestJS Kafka transport.** If its producer ergonomics prove limiting,
  dropping to `kafkajs` directly is contained to one module. Producer-only mode avoids the
  request/response reply-topic overhead the transport otherwise adds.
- Light validation only at this stage; real schema validation is Phase 1 (ADR to follow if
  a schema-registry decision is made).
- **KafkaJS patch:** KafkaJS 2.2.4 (its last release) schedules an idle `setTimeout` with a
  negative delay in `RequestQueue.scheduleCheckPendingRequests`, emitting Node's
  `TimeoutNegativeWarning` and re-arming a pointless ~1 ms timer. We carry a one-line fix
  via `patch-package` (`patches/kafkajs+2.2.4.patch`, applied on `postinstall`) rather than
  suppressing the warning, so service logs stay clean. Revisit if we migrate off KafkaJS.
