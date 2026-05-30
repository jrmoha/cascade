# C4 Container Diagram — Cascade

**FigJam board:**
[Cascade — C4 Container View](https://www.figma.com/board/HjsO5KJ5neQHDJneNLxMjt/Cascade--C4-Container-View)

---

## What the diagram shows

The diagram is a [C4 container-level view](https://c4model.com/#ContainerDiagram): one level below the system context, showing the runnable units (services and data stores) and the connections between them.

### Containers

| Container                | Type           | Technology            |
| ------------------------ | -------------- | --------------------- |
| Client / Game SDK        | External actor | Any HTTP client       |
| Collector                | Service        | NestJS                |
| Kafka `raw-events` topic | Message broker | Apache Kafka          |
| Ingestion-Processor      | Service        | NestJS                |
| Cassandra                | Data store     | Apache Cassandra      |
| Aggregator               | Service        | NestJS                |
| Redis                    | Data store     | Redis                 |
| Query API                | Service        | NestJS                |
| Project / Schema service | Service        | NestJS                |
| PostgreSQL               | Data store     | PostgreSQL            |
| Dashboards               | External actor | Browser (built later) |

---

## Write path (write-heavy, append-only)

```
Client / Game SDK
    │  events (HTTP)
    ▼
Collector  ──────────────────────────────────► Project/Schema service
(validate + produce)   validate schema / API key        │
    │                                                    └──► PostgreSQL
    │  produce
    ▼
Kafka (raw-events topic)
    │  consume
    ▼
Ingestion-Processor
    │  write raw (append-only)
    ▼
Cassandra
```

The Collector's only job is schema validation and Kafka publication. It owns no data store.
The Ingestion-Processor is the sole writer to Cassandra.

---

## Read path (read-heavy, low-latency)

```
Kafka (raw-events topic)
    │  consume same stream (independent consumer group)
    ▼
Aggregator
(windowed rollups)
    │  derive views
    ▼
Read models (Redis counters · PostgreSQL aggregated views)
    ▲
    │  read pre-aggregates
Query API ◄──── Dashboards (query)
    │
    └──── Project/Schema service (gRPC)
```

The Query API **never** reads from Cassandra. All queries are served from Redis or PostgreSQL
read models that the Aggregator maintains continuously.

---

## Key architectural boundary

The diagram makes the core constraint visible: Cassandra sits entirely on the write side. There is
no arrow from the Query API to Cassandra. Any proposed feature that would require such an arrow is
a design error — the missing aggregation must be added to the Aggregator instead.

See [ADR-0001](../adr/0001-overall-architecture.md) for the full rationale.
