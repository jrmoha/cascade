# Runbook: Query API

Read-path HTTP service (NestJS). The read side of Cascade's CQRS split. It serves
two kinds of read:

- **Analytics** — `GET /counts` (Cassandra counter tables), `GET /leaderboard` +
  `GET /leaderboard/rank` (Redis), `GET /funnel` + `GET /retention` (Postgres).
  Each reads **only** the Aggregator's derived views, never `raw_events`. These
  views are **eventually consistent**: a freshly ingested event appears only
  after the Aggregator processes it (seconds) — expected, not a bug. The boundary
  (analytics never reads raw) is enforced by `test/cqrs-boundary.spec.ts`. See
  [ADR-0015](../adr/0015-read-model-aggregation-strategy.md) /
  [ADR-0018](../adr/0018-enforce-cqrs-read-boundary.md) and the per-view pages
  under `docs/read-models/`.
- **Bounded raw retrieval** — `GET /query`, below.

> **`GET /query` is bounded raw retrieval, not analytics.** It reads **raw events
> directly from Cassandra** for replay/audit/debugging. It is partition-key-
> bounded — never a cross-partition scan, never `ALLOW FILTERING`. Analytics is
> served separately from Aggregator read models and never touches raw Cassandra.
> See [ADR-0008](../adr/0008-raw-event-time-range-read.md) (supersedes the
> Phase-0 ADR-0003) and `docs/contracts/events.md` → "Reading events back".

## API

### Analytics endpoints (derived views only)

| Endpoint                | Query params                                 | Served from                                     |
| ----------------------- | -------------------------------------------- | ----------------------------------------------- |
| `GET /counts`           | `projectId, from, to, [granularity], [type]` | `event_counts_by_minute`/`_by_hour` (Cassandra) |
| `GET /leaderboard`      | `projectId, period, [limit]`                 | `lb:{projectId}:{period}` sorted set (Redis)    |
| `GET /leaderboard/rank` | `projectId, period, playerId`                | same Redis sorted set (404 if player absent)    |
| `GET /funnel`           | `projectId, steps, from, to`                 | `funnel_actor_steps` (Postgres)                 |
| `GET /retention`        | `projectId, from, to, [maxOffset]`           | `retention_actor_activity` (Postgres)           |

`GET /counts` returns a newest-first series of `{ bucket, eventType, count }`.
`granularity` (`minute`|`hour`, default `hour`) selects the counter table; span is
capped so fan-out is bounded (`hour` ≤ 168 buckets / 7 days, `minute` ≤ 1440 / 24 h);
`from > to` or an over-cap window is `400`. Full request/response shapes are in
[`query-api.openapi.yaml`](../specs/query-api.openapi.yaml); the per-view mechanics are
in `docs/read-models/`.

All five are **eventually consistent** with ingestion (they reflect only what the
Aggregator has processed) and never read `raw_events`.

### Bounded raw retrieval

`GET /query` — read a project's events within an inclusive event-time window
`[from, to]`, newest-first, cursor-paged (KAN-25).

| Query param | Required | Default | Notes                                                              |
| ----------- | -------- | ------- | ------------------------------------------------------------------ |
| `projectId` | yes      | —       | Tenant/project to read events for.                                 |
| `from`      | yes      | —       | ISO-8601. Inclusive lower bound on `occurredAt` (event time).      |
| `to`        | yes      | —       | ISO-8601. Inclusive upper bound on `occurredAt`. Must be ≥ `from`. |
| `limit`     | no       | `100`   | Page size. Max `1000`.                                             |
| `cursor`    | no       | —       | Opaque continuation token from a prior response's `nextCursor`.    |

Reads are **partition-key-bounded**: the window is mapped to the hourly
`time_bucket` partitions it covers, and each is read with one prepared,
`occurred_at`-bounded single-partition `SELECT ... WHERE project_id = ? AND
time_bucket = ? AND occurred_at >= ? AND occurred_at <= ?`. Results are
newest-first (the table's `CLUSTERING ORDER BY (occurred_at DESC, …)` provides
the order — see ADR-0007). No `ALLOW FILTERING`. A window may span at most
**168** hourly buckets (7 days); wider is rejected with `400`.

Response:

```json
{
  "projectId": "game-1",
  "from": "2026-05-30T15:00:00.000Z",
  "to": "2026-05-30T15:59:59.999Z",
  "count": 1,
  "events": [
    {
      "eventId": "11111111-1111-4111-8111-111111111111",
      "projectId": "game-1",
      "type": "level_complete",
      "occurredAt": "2026-05-30T15:10:00.000Z",
      "receivedAt": "2026-05-30T15:10:00.500Z",
      "payload": { "level": 3 }
    }
  ]
}
```

**Pagination:** when more rows may remain, the response includes a `nextCursor`;
pass it back as `cursor` to fetch the next page. **Stop when `nextCursor` is
absent** (a present cursor does not guarantee further rows). Treat the cursor as
opaque.

`400` on: missing/non-ISO `projectId`/`from`/`to`, `from` after `to`, a window
wider than 168 buckets, or a malformed/mismatched `cursor`. An unknown
`projectId` returns `200` with `count: 0`.

## Configuration

| Env var                    | Default       | Notes                                                                                               |
| -------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
| `PORT`                     | `3002`        | HTTP listen port.                                                                                   |
| `CASSANDRA_CONTACT_POINTS` | _required_    | Comma-separated. `cassandra:9042` in-container. Serves `/counts` + `/query`.                        |
| `CASSANDRA_PORT`           | `9042`        |                                                                                                     |
| `CASSANDRA_LOCAL_DC`       | `datacenter1` | Must match the cluster's datacenter. With the default SimpleSnitch it is `datacenter1` (NOT `dc1`). |
| `REDIS_HOST`               | _required_    | Redis host. Serves `/leaderboard` + `/leaderboard/rank`.                                            |
| `REDIS_PORT`               | `6379`        |                                                                                                     |
| `DATABASE_URL`             | _required_    | Postgres connection string (read-only pool). Serves `/funnel` + `/retention`.                       |

The service is **read-only** across all three stores — it performs no DDL or
migrations. The Ingestion-Processor owns the `cascade.raw_events` schema and the
Aggregator owns the derived-view schemas (Cassandra counter tables, Redis keys,
Postgres funnel/retention tables); the Query API must not be started expecting it
to create anything. All three stores are readiness deps — `GET /ready` returns
`503` if any is down.

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

# 3. read it back through the Query API (no cqlsh, no partition key needed).
#    Window the last hour up to now (covers the current + previous bucket).
FROM=$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)   # GNU date: date -u -d '1 hour ago' +...
TO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s "localhost:3002/query?projectId=kan19-demo&from=$FROM&to=$TO" | jq
```

The posted event comes back in `events`. For a large window, page with
`&limit=100` and follow `nextCursor` until it is absent.

## Tests

```bash
SKIP_INTEGRATION=1 npm test -w @cascade/query-api   # unit only (no Docker)
npm test -w @cascade/query-api                      # + Testcontainers Cassandra
```

The integration test starts a real Cassandra container, seeds rows the way the
Ingestion-Processor would, then reads them back through the live `GET /query`
endpoint — the DB is never mocked.
