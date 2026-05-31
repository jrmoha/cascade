# Runbook: Collector

Thin write-path service. Accepts events on `POST /collect` and produces them to the Kafka
`raw-events` topic. See [contracts/events.md](../contracts/events.md).

## Configuration

| Env var                   | Default          | Notes                                                                                  |
| ------------------------- | ---------------- | -------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated. Use `localhost:9092` from the host, `kafka:29092` from inside Docker. |
| `PORT`                    | `3001`           | HTTP listen port.                                                                      |

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
