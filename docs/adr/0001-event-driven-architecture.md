# 0001 — Event-driven service topology with Kafka

**Status:** Accepted

## Context

Cascade has two workloads with opposite characteristics:

- **Write path**: high-volume, bursty, append-only event ingestion. Needs to absorb traffic spikes without blocking producers. Latency on the write side is tolerably loose (sub-second is fine).
- **Read path**: dashboard queries that need pre-shaped, low-latency answers. Cannot afford to scan raw data live.

A naive single-database design (e.g. write events to PostgreSQL and query them) fails at both ends: PostgreSQL becomes a write bottleneck under load, and live aggregation queries over millions of raw rows are too slow.

The alternatives considered were:

1. **Synchronous write-through with a cache**: Collector writes to Cassandra directly, an in-process cache warms read models. Simple, but the Collector becomes the aggregation engine — it couples concerns, and a burst of writes starves reads.
2. **Polling-based aggregation**: a background job periodically reads raw events and recomputes aggregates. Simpler infrastructure, but adds latency proportional to polling interval and re-reads data unnecessarily.
3. **Event-driven with Kafka**: Collector publishes to Kafka; independent consumers handle raw storage and aggregation separately. Decoupled, replayable, horizontally scalable.

## Decision

Use **Kafka as the backbone** between the Collector and all downstream processors. The Collector's only job is to validate and publish. Two independent consumer groups — Ingestion-Processor and Aggregator — consume from the same topics for their own purposes.

- **Ingestion-Processor** writes every raw event to Cassandra (append-only, query-first modelled).
- **Aggregator** updates Redis counters and PostgreSQL read models as events arrive.
- **Query API** reads exclusively from Redis and PostgreSQL — it never touches Cassandra.

PostgreSQL holds project metadata and structured aggregated views (funnels, retention). Redis holds hot counters, leaderboards, and rate-limit buckets.

## Consequences

**Positive:**

- Write path and read path are fully decoupled and scale independently.
- Kafka provides durability and replay — a consumer can reprocess history without re-ingesting from clients.
- Consumers are idempotent by design (at-least-once delivery requires this), which improves resilience.
- Each service has a single, well-defined responsibility.

**Negative / trade-offs:**

- More infrastructure to operate: Kafka, Cassandra, Redis, PostgreSQL all run alongside the services.
- Eventual consistency between raw storage and aggregated views — there is a small lag between an event arriving and its aggregation being visible.
- Idempotency must be implemented and tested explicitly; it is not free.
- Local development requires Docker Compose to spin up all dependencies.

The complexity is judged worthwhile because the decoupling is the core engineering challenge this project is designed to explore (see [00-charter.md](../00-charter.md) §3).
