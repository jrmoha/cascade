# Runbook: Kafka partitioning & consumer-group scaling

`raw-events` runs on a **3-broker cluster** with **6 partitions / RF=3 / min.insync.replicas=2**
(KAN-40, [ADR-0020](../adr/0020-kafka-partitioning-and-scaling.md)). This page covers the topology,
how topics are provisioned, scaling the consumer groups, reading partition assignment, observing a
rebalance and the throughput payoff, and the gotchas.

## Topology

- **3 brokers** `kafka-1/2/3` (KRaft combined broker+controller) in `infra/docker-compose.yml`,
  sharing a 3-voter metadata quorum. Host listeners: `localhost:9092` / `9094` / `9095`; internal
  `kafka-N:29092`.
- **`raw-events`: 6 partitions, RF=3, `min.insync.replicas=2`.** `raw-events.dlq`: 3 partitions,
  RF=3. Offsets/txn topics RF=3.
- **Producers use `acks=all`** (KafkaJS default) so a write needs 2 of 3 in-sync replicas — one
  broker down stays writable and loses no acknowledged data (the Kafka parallel to Cassandra's
  `R+W>RF`, ADR-0019).
- **Partition key = `sessionId ?? actorId ?? eventId`** (`services/collector/src/collector/collector.service.ts`):
  a session stays ordered on one partition; a busy project spreads across all six.

## Topics are provisioned explicitly (auto-create is OFF)

Broker `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, so nothing silently creates a wrong 1-partition
topic. The one-shot **`kafka-init`** service creates both topics with their partition/RF on `make up`
and exits; every app service `depends_on` it (`condition: service_completed_successfully`).

```bash
make up   # brings up 3 brokers + kafka-init (+ Cassandra, Redis, Postgres)

# Verify (expect PartitionCount: 6, ReplicationFactor: 3, leaders spread 1/2/3):
docker exec cascade-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --describe --topic raw-events
```

To change the partition count later you must `kafka-topics.sh --alter` (you can only **increase**,
and it rehashes future keys) — never by editing `kafka-init` after the fact.

## Scale the consumer groups

The Ingestion-Processor and Aggregator have no `container_name`/host-port binding, so they scale:

```bash
make stack-scale                 # 3 Ingestion-Processor + 2 Aggregator replicas (defaults)
make stack-scale IP=6 AGG=3      # override counts

# or directly:
docker compose -f infra/docker-compose.yml --profile apps up -d \
  --scale ingestion-processor=3 --scale aggregator=2
```

Each replica joins its **single** consumer group; Kafka distributes the 6 partitions across the live
members. Parallelism is capped at the partition count — a **7th** Ingestion-Processor would sit
**idle** (no partition to own).

## Read the partition assignment

**Gotcha:** NestJS `ServerKafka` postfixes the broker-side group id with `-server`. Use
`cascade-ingestion-processor-server` / `cascade-aggregator-server`:

```bash
GRP=cascade-ingestion-processor-server
BS=localhost:29092
# Members and how many partitions each owns:
docker exec cascade-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS \
  --group $GRP --describe --members
# Per-partition current/end offset and LAG:
docker exec cascade-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS \
  --group $GRP --describe
```

At 3 members you should see ~2 partitions each; no single member owns all 6.

## The demo (one command)

```bash
./infra/scripts/kafka-scaling-demo.sh          # BACKLOG=40000 by default
```

It uses `infra/scripts/kafka-load.mjs` (synthetic **valid** `RawEvent`s produced straight to
`raw-events`, bypassing the Collector) and, against the real Ingestion-Processor group, shows:

1. **Topic layout** — 6 partitions, RF=3, leaders spread across brokers.
2. **Per-key ordering** — 500 events for a _single_ `sessionId` all land on **one** partition.
3. **Throughput @ 1 vs 3 instances** — times draining a fixed backlog (group LAG → 0) at each scale;
   the drain rate rises toward the partition-count cap.
4. **Rebalance** — scaling 1→3 reassigns partitions and processing continues (safe because every
   Aggregator write is idempotent — [ADR-0016](../adr/0016-idempotent-replayable-aggregation.md)).

## Observe a rebalance directly

```bash
# Tail a consumer while you scale — watch it log partitions revoked/assigned:
docker compose -f infra/docker-compose.yml logs -f ingestion-processor &
make stack-scale IP=3        # then IP=1 to shrink — both trigger a rebalance
```

A rebalance briefly pauses consumption and may **redeliver** the last uncommitted messages; offsets
commit only **after** the durable write (KAN-33), so nothing is lost or double-counted.

## Broker-loss durability (RF=3 + acks=all)

```bash
docker stop cascade-kafka-2                       # one broker down
# Producing still succeeds (2 of 3 in-sync ≥ min.insync=2) and consumers keep going:
node infra/scripts/kafka-load.mjs                 # completes with no errors
docker start cascade-kafka-2                       # rejoins; ISR heals
```

Losing a **second** broker drops in-sync replicas below `min.insync.replicas=2`, so `acks=all`
produces fail — the deliberate durability boundary (as with Cassandra quorum, ADR-0019).

## Gotchas

- **`-server` group suffix** — the biggest footgun with `kafka-consumer-groups.sh`.
- **Partitions can't shrink**, and increasing them rehashes future keys — pick the count with
  headroom (we chose 6).
- **Scaling past the partition count idles the extra consumers** (6 partitions ⇒ ≤ 6 useful
  Ingestion-Processors per group).
- **Throughput is bounded by the slowest hop.** If the Ingestion-Processor's Cassandra writes are the
  bottleneck, adding consumers helps until Cassandra saturates — increase `BACKLOG` in the demo to
  see a clear gap.
