# Runbook: Query API

Read-path HTTP service. A NestJS app that serves `GET /query`, returning stored
events for a project.

> **Phase 0 shortcut.** This endpoint reads **raw events directly from
> Cassandra** to close the ingest→store→read loop (KAN-19). That deliberately
> deviates from the target design, where the Query API reads only pre-aggregated
> read models and never touches Cassandra. It is temporary and removed in Phase 1.
> See [ADR-0003](../adr/0003-query-api-phase0-raw-read.md).

## API

`GET /query`

| Query param | Required | Default | Notes                                                          |
| ----------- | -------- | ------- | -------------------------------------------------------------- |
| `projectId` | yes      | —       | Tenant/project to read events for.                             |
| `hours`     | no       | `1`     | Hourly-bucket lookback (1 = current hour only). Integer 1–168. |

Reads are **partition-key-bounded**: one prepared single-partition
`SELECT ... WHERE project_id = ? AND time_window = ?` per hourly bucket, merged
and returned newest-first. No `ALLOW FILTERING`.

Response:

```json
{
  "projectId": "game-1",
  "hours": 1,
  "count": 1,
  "events": [
    {
      "eventId": "11111111-1111-4111-8111-111111111111",
      "projectId": "game-1",
      "type": "level_complete",
      "timestamp": "2026-05-30T15:10:00.000Z",
      "payload": { "level": 3 }
    }
  ]
}
```

A missing `projectId` returns `400`. An unknown `projectId` returns `200` with
`count: 0`.

## Configuration

| Env var                    | Default       | Notes                                                                                               |
| -------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
| `PORT`                     | `3002`        | HTTP listen port.                                                                                   |
| `CASSANDRA_CONTACT_POINTS` | `localhost`   | Comma-separated. `cassandra:9042` in-container.                                                     |
| `CASSANDRA_PORT`           | `9042`        |                                                                                                     |
| `CASSANDRA_LOCAL_DC`       | `datacenter1` | Must match the cluster's datacenter. With the default SimpleSnitch it is `datacenter1` (NOT `dc1`). |

The service is **read-only** — it performs no DDL. The Ingestion-Processor owns
the `cascade.raw_events` schema (see its runbook); the Query API must not be
started against an empty cluster expecting it to create tables.

## Run locally

```bash
make up   # ensure cassandra is healthy
docker inspect -f '{{.State.Health.Status}}' cascade-cassandra   # -> healthy

npm run build -w @cascade/contracts
npm run build -w @cascade/query-api
npm run start:dev -w @cascade/query-api
# or: CASSANDRA_CONTACT_POINTS=localhost PORT=3002 node services/query-api/dist/main.js
```

## Verify end-to-end (full pipe)

```bash
# 1. processor + collector running (see ingestion-processor runbook), query-api running
# 2. post an event
curl -s -X POST localhost:3001/collect -H 'content-type: application/json' \
  -d '{"projectId":"kan19-demo","type":"boss_defeated","payload":{"boss":"dragon"}}'

# 3. read it back through the Query API (no cqlsh, no partition key needed)
curl -s 'localhost:3002/query?projectId=kan19-demo' | jq
```

The posted event comes back in `events`. If the request happens to straddle an
hour boundary, widen the lookback: `?projectId=kan19-demo&hours=2`.

## Tests

```bash
SKIP_INTEGRATION=1 npm test -w @cascade/query-api   # unit only (no Docker)
npm test -w @cascade/query-api                      # + Testcontainers Cassandra
```

The integration test starts a real Cassandra container, seeds rows the way the
Ingestion-Processor would, then reads them back through the live `GET /query`
endpoint — the DB is never mocked.
