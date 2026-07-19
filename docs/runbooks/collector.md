# Runbook: Collector

Thin write-path service. Accepts events on `POST /collect` and produces them to the Kafka
`raw-events` topic. See [contracts/events.md](../contracts/events.md).

## Configuration

| Env var                   | Default          | Notes                                                                                  |
| ------------------------- | ---------------- | -------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated. Use `localhost:9092` from the host, `kafka:29092` from inside Docker. |
| `PORT`                    | `3001`           | HTTP listen port.                                                                      |

Resilience knobs (KAN-42, ADR-0021 — all optional, defaulted; full list in `.env.example`):

| Env var                                                      | Default        | Controls                                          |
| ------------------------------------------------------------ | -------------- | ------------------------------------------------- |
| `RATE_LIMIT_REFILL_PER_SEC` / `RATE_LIMIT_BURST`             | 50 / 100       | Per-API-key token bucket (sustained rate / burst) |
| `PRODUCE_MAX_INFLIGHT`                                       | 500            | Backpressure cap on concurrent produces           |
| `PRODUCE_TIMEOUT_MS`                                         | 5000           | Per-produce-attempt deadline                      |
| `PRODUCE_MAX_ATTEMPTS` / `PRODUCE_RETRY_BASE_MS`             | 3 / 100        | Produce retry count / backoff base                |
| `PROJECT_SCHEMA_BREAKER_ERROR_PCT` / `_RESET_MS` / `_VOLUME` | 50 / 10000 / 5 | Project/Schema circuit breaker                    |

## Resilience (KAN-42, ADR-0021)

The ingest edge sheds load and tolerates a slow dependency rather than falling over. Status codes:

| Code              | Meaning                                                                                                                |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `202`             | Accepted — handed to Kafka.                                                                                            |
| `429`             | Per-API-key rate limit exceeded (token bucket). Honour the `Retry-After` header.                                       |
| `503`             | Backpressure (in-flight cap reached) **or** produce retries exhausted. The event was **not** accepted — safe to retry. |
| `401`/`400`/`422` | Auth / envelope / schema validation (unchanged, KAN-30).                                                               |

- **Rate limiting** is per key (SHA-256 bucket in Redis), applied **before** auth, and **fails open**
  if Redis is unreachable — it is a spike shield, not a security gate.
- **Backpressure + retry** never silently drop data: a `503` means the event was never acknowledged,
  so the client still owns it. There is no Collector-side DLQ (a dead Kafka can't take one).
- **Circuit breaker** wraps the Project/Schema gRPC calls (`opossum`). When open, cold requests
  fail-closed **fast** (no 5 s hang) and warm-cache hits are still served; `NOT_FOUND` (unregistered
  schema → `422`) never trips it. Watch the logs for `Circuit OPEN/HALF-OPEN/CLOSED` transitions.

Load-testing these paths under a spike: see [load-testing.md](load-testing.md) (`make load-test`).

## Run locally

```bash
# 1. Bring up infra (Kafka et al.) and wait for Kafka to report healthy
make up
docker inspect -f '{{.State.Health.Status}}' cascade-kafka   # -> healthy

# 2. Build shared contracts, then the service
npm install
npm run build -w @cascade/contracts
npm run build -w @cascade/collector

# 3. Start the collector (dev mode with reload)
npm run start:dev -w @cascade/collector
# or production-style:
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 PORT=3001 node services/collector/dist/main.js
```

## Verify end-to-end (console consumer)

```bash
# Terminal A — tail the topic
docker exec cascade-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-events \
  --from-beginning --property print.key=true

# Terminal B — post an event
curl -i -X POST localhost:3001/collect \
  -H 'content-type: application/json' \
  -d '{"projectId":"game-1","type":"level_complete","payload":{"level":3}}'
# -> 202 Accepted, body: {"eventId":"...","status":"accepted"}
```

Terminal A should print a message keyed `game-1` with the matching `eventId`, a
server-stamped `timestamp`, and the payload.

A missing/empty `projectId` or `type` returns `400` and produces nothing.

## Tests

```bash
# Unit only (no Docker needed)
SKIP_INTEGRATION=1 npm test -w @cascade/collector

# Full suite — includes a Testcontainers Kafka integration test (Docker required)
npm test -w @cascade/collector
```

The integration test starts a real Kafka (KRaft) container, posts to `/collect`, and
asserts the message arrives on `raw-events` — the broker is never mocked.
