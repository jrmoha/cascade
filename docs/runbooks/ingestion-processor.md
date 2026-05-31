# Runbook: Ingestion-Processor

Write-path consumer. A NestJS Kafka microservice that consumes the `raw-events`
topic (consumer group `cascade-ingestion-processor`) and appends each event to
Cassandra. See [contracts/events.md](../contracts/events.md). Events that fail processing are
routed to a dead-letter topic — see [dlq.md](dlq.md).

## Configuration

| Env var                    | Default          | Notes                                                                                               |
| -------------------------- | ---------------- | --------------------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092` | `localhost:9092` from host, `kafka:29092` in-container.                                             |
| `CASSANDRA_CONTACT_POINTS` | `localhost`      | Comma-separated. `cassandra:9042` in-container.                                                     |
| `CASSANDRA_PORT`           | `9042`           |                                                                                                     |
| `CASSANDRA_LOCAL_DC`       | `datacenter1`    | Must match the cluster's datacenter. With the default SimpleSnitch it is `datacenter1` (NOT `dc1`). |

## Schema

The service ensures the keyspace + table on startup (`IF NOT EXISTS`). Reference DDL:
[`services/ingestion-processor/src/cassandra/schema.cql`](../../services/ingestion-processor/src/cassandra/schema.cql).

- **Table:** `cascade.raw_events`
- **Partition key:** `(project_id, time_window)` — `time_window` is the hourly UTC bucket `YYYY-MM-DDTHH`, derived from `occurred_at` (event time).
- **Clustering key:** `event_id` — makes `INSERT` an idempotent upsert.
- **Columns:** `type`, `occurred_at` (event time), `received_at` (ingest time), `payload`, and nullable `session_id` / `actor_id` / `source`.
- **Query it serves:** `SELECT * FROM cascade.raw_events WHERE project_id = ? AND time_window = ?`

> **Schema migration (KAN-21):** the envelope split `event_time` into `occurred_at` +
> `received_at` and added the optional columns. `CREATE TABLE IF NOT EXISTS` will **not** alter
> a pre-existing local table — if you have persistent dev data, recreate it once with
> `docker compose -f infra/docker-compose.yml down -v` (RF=1 throwaway data). Tests use ephemeral
> containers and are unaffected.

## Run locally

```bash
make up   # ensure cassandra + kafka are healthy
docker inspect -f '{{.State.Health.Status}}' cascade-cassandra   # -> healthy

npm run build -w @cascade/contracts
npm run build -w @cascade/ingestion-processor
npm run start:dev -w @cascade/ingestion-processor
# or: KAFKA_BOOTSTRAP_SERVERS=localhost:9092 CASSANDRA_CONTACT_POINTS=localhost \
#     node services/ingestion-processor/dist/main.js
```

The consumer group starts at the latest offset, so start the processor **before**
producing the events you want to see.

## Verify end-to-end (with the Collector)

```bash
# 1. processor running (above); 2. start the collector
PORT=3001 node services/collector/dist/main.js &

# 3. post an event
curl -s -X POST localhost:3001/collect -H 'content-type: application/json' \
  -d '{"projectId":"kan18-demo","type":"boss_defeated","payload":{"boss":"dragon"}}'

# 4. read it back (use the full partition key — project_id + the hourly window)
docker exec cascade-cassandra cqlsh -e \
  "SELECT * FROM cascade.raw_events WHERE project_id='kan18-demo' AND time_window='$(date -u +%Y-%m-%dT%H)';"
```

A row appears with the matching `event_id`, hourly `time_window`, `occurred_at`,
`received_at`, and JSON `payload`. Re-delivering the same event leaves exactly one row
(idempotent upsert).

> Querying by `project_id` alone fails with "use ALLOW FILTERING" — that is the
> query-first model working as intended; always query with the full partition key.

## Tests

```bash
SKIP_INTEGRATION=1 npm test -w @cascade/ingestion-processor   # unit only (no Docker)
npm test -w @cascade/ingestion-processor                      # + Testcontainers Cassandra
```

The integration test starts a real Cassandra container, writes via the repository,
reads rows back, and asserts the idempotent-upsert behavior — the DB is never mocked.
