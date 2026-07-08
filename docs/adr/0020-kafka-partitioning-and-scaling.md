# 0020 — Kafka partitioning & consumer-group scaling

**Status:** Accepted

## Context

KAN-40 opens the **Kafka half** of Phase 4 (scaling & resilience — KAN-9), after KAN-38/39 did
the Cassandra half (multi-node RF=3, node-loss chaos). Until now `raw-events` was a **single
auto-created topic** — **1 partition, RF=1, on a single broker** — consumed by **one instance** each
of the Ingestion-Processor (`cascade-ingestion-processor`) and Aggregator (`cascade-aggregator`).

That shape has three problems Phase 4 exists to fix:

- **No parallelism.** One partition ⇒ at most one useful consumer per group. Ingestion throughput
  can't scale by adding consumers.
- **No broker durability.** RF=1 ⇒ losing the broker (or its disk) loses the log — the Kafka
  equivalent of the Cassandra RF=1 we already rejected in [ADR-0019](0019-replication-and-consistency-model.md).
- **Rebalancing is invisible.** With one partition and one consumer, partition assignment and
  rebalancing — core operational behaviour — never happen, so they're neither understood nor tested.

The partition key was `projectId` ([ADR-0009](0009-service-boundaries-and-communication.md) §3),
which co-locates a project's events but pins a **single busy project to one partition** — so adding
consumers can't speed up the hot tenant that most needs it.

## Decision

### 1. Topology — 3 brokers, `raw-events` = 6 partitions, RF=3, `min.insync.replicas=2`

- **3-broker KRaft cluster** (`kafka-1/2/3`, combined broker+controller, `infra/docker-compose.yml`).
  A 3-voter metadata quorum, the Kafka parallel to the 3-node Cassandra ring.
- **`raw-events`: 6 partitions.** Parallelism is capped by partition count (N partitions ⇒ ≤ N
  useful consumers per group), and **you can't reduce partitions later** (and increasing them breaks
  key→partition stability for existing keys). 6 gives headroom over today's 1–2 consumers without
  over-sharding a dev cluster. `raw-events.dlq`: **3 partitions** (low volume).
- **RF=3, `min.insync.replicas=2`, producers `acks=all`.** The read/write overlap that makes it
  durable: with `acks=all` a produce isn't acknowledged until 2 of 3 in-sync replicas have it, so a
  **single broker loss keeps the topic writable and loses no acknowledged data** — the same
  `survive one node down` property proved for Cassandra in KAN-39, one system down (ADR-0019 §0).
  Offsets/txn-state topics are RF=3 too.
- **Auto-create is OFF.** Topics are provisioned **explicitly** with their partition/RF by a
  one-shot `kafka-init` job, so a consumer subscribing early can never silently create a wrong
  1-partition topic. (Mirrors "never ad-hoc `CREATE TABLE`" for Cassandra — ADR-0007; production
  provisions via IaC in Phase 5.)

### 2. Partition key — `sessionId ?? actorId ?? eventId` (was `projectId`)

The Collector now keys each event by **`sessionId ?? actorId ?? eventId`** (`collector.service.ts`).
Rationale:

- **Spreads a hot tenant.** A single busy project's events fan out across all 6 partitions, so
  adding consumers actually raises its throughput — the whole point of the ticket.
- **Preserves the ordering that matters.** Kafka only guarantees order **within a partition**.
  Keying by `sessionId` keeps a **session's** events ordered on one partition — exactly the ordering
  the KAN-35 funnel reasons about (per-actor step sequence). Per-_project_ global order was never
  needed.
- **Correctness is unaffected.** Every Aggregator write is **commutative/idempotent**
  ([ADR-0016](0016-idempotent-replayable-aggregation.md)): counts are additive, the leaderboard is
  `ZADD GT`, funnel/retention are `LEAST` / `ON CONFLICT DO NOTHING`. So spreading a project across
  partitions (and consumers) changes nothing about the derived views.
- **Never undefined.** `eventId` is always present, so the fallback chain always yields a key (an
  event with neither session nor actor is spread by its unique id — it has no sequence to preserve).
- **DLQ stays keyed by `projectId`.** Dead letters are low-volume and best co-located per project
  for inspection; ordering is irrelevant there.

### 3. Scaling — multiple instances per consumer group

The Ingestion-Processor and Aggregator run as **N replicas in their single consumer group**
(`docker compose --scale`, `make stack-scale`). Kafka assigns the 6 partitions across the live
members; adding/removing an instance triggers a **rebalance** that reassigns partitions. This leans
directly on the idempotency contract: a rebalance briefly pauses consumption and can **redeliver**
the last uncommitted messages, and reprocessing must not double-count — which KAN-33
([ADR-0016](0016-idempotent-replayable-aggregation.md)) already guarantees and proves. Offsets
commit **after** the durable write, so a partition revoked mid-flight is re-consumed, not lost.

**Operational note:** NestJS `ServerKafka` postfixes the broker-side group id with `-server`
(`cascade-ingestion-processor-server`, `cascade-aggregator-server`) — use those names with
`kafka-consumer-groups.sh`.

## Alternatives considered

- **Keep `projectId` as the key.** Preserves per-project order and co-location, but a single hot
  project can't be parallelized (all its events on one partition) — it fails the "throughput rises
  with instances" goal for the exact workload that needs it. Rejected; per-project order wasn't
  required and the aggregations are commutative.
- **Key by `eventId` (pure spread, no ordering).** Maximal spread but throws away per-session order
  the funnel benefits from, for no gain over the chosen fallback chain. Rejected.
- **More partitions (e.g. 12/24).** More headroom, but over-shards a dev cluster (more metadata,
  more open files, tiny partitions) for parallelism we're nowhere near needing. 6 is enough now and
  partitions can be _increased_ later (accepting key-rehash) if load demands. Rejected for now.
- **RF=2 / 2 brokers.** Lighter, still "RF>1", but RF=2 + min.insync=2 can't stay writable through a
  broker loss — a weaker durability story than the Cassandra one it's meant to parallel. Rejected.
- **Auto-create topics.** Convenient but yields 1-partition topics by default and hides the
  provisioning decision; a consumer racing the producer could create the topic wrong. Rejected in
  favour of explicit `kafka-init`.

## Consequences

- `infra/docker-compose.yml` runs a 3-broker cluster + a `kafka-init` provisioner; app services use
  `KAFKA_BOOTSTRAP_SERVERS=kafka-1:29092,kafka-2:29092,kafka-3:29092`. The Ingestion-Processor and
  Aggregator lost their `container_name`/host-port bindings so `--scale` can run replicas.
- **Ordering guarantee changed:** per-**session** (was per-project). [ADR-0009](0009-service-boundaries-and-communication.md)
  §3 is updated (topic inventory now records partitions/RF and the new key); the `RawEvent` contract
  doc note is updated to match.
- **Producer durability:** `acks=all` + RF=3 + min.insync=2 tolerates one broker down. A tooling
  parallel to Cassandra's `R+W>RF`.
- **Demonstrated, not asserted:** `infra/scripts/kafka-load.mjs` + `infra/scripts/kafka-scaling-demo.sh`
  show 6-partition layout, per-key ordering (a session on one partition), throughput rising from 1→3
  instances, and a live rebalance. See `docs/runbooks/kafka-scaling.md`. Multi-broker/multi-instance
  behaviour is compose-exercised (not CI), like the Cassandra cluster; KAN-33's idempotency e2e
  already covers redelivery-on-rebalance correctness.
- **No contract/envelope change** (`RAW_EVENT_SCHEMA_VERSION` unchanged): the partition key is a
  Kafka-level routing concern, not a field of the event.

This ADR builds on [ADR-0002](0002-collector-kafka-production.md) (Collector→Kafka, DefaultPartitioner),
[ADR-0009](0009-service-boundaries-and-communication.md) (topics & boundaries),
[ADR-0016](0016-idempotent-replayable-aggregation.md) (idempotency under redelivery), and mirrors the
replication posture of [ADR-0019](0019-replication-and-consistency-model.md).
