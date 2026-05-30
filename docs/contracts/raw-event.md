# Contract: `RawEvent` / `raw-events` topic

The envelope the **Collector** produces onto the Kafka `raw-events` topic, consumed
independently by the Ingestion-Processor and the Aggregator.

Source of truth: [`libs/contracts/src/events.ts`](../../libs/contracts/src/events.ts)
(`@cascade/contracts`). Topic name is exported as `RAW_EVENTS_TOPIC = 'raw-events'`.

## Kafka message

| Aspect | Value                                                                 |
| ------ | --------------------------------------------------------------------- |
| Topic  | `raw-events`                                                          |
| Key    | `projectId` (UTF-8 string) — pins a project's events to one partition |
| Value  | JSON-serialized `RawEvent` (UTF-8)                                    |

## Envelope

| Field       | Type                | Required on the wire | Notes                                                                                                                                          |
| ----------- | ------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `eventId`   | `string` (UUID v4)  | always present       | Server-stamped by the Collector if the client omits it. Stable idempotency key for downstream dedup (Cassandra clustering key — see ADR-0001). |
| `projectId` | `string`            | yes                  | Tenant id; also the Kafka partition key.                                                                                                       |
| `type`      | `string`            | yes                  | Event type discriminator, e.g. `level_complete`.                                                                                               |
| `timestamp` | `string` (ISO-8601) | always present       | Defaulted to ingestion time by the Collector if absent.                                                                                        |
| `payload`   | `object`            | always present       | Arbitrary event data; defaults to `{}`. Opaque to the Collector in Phase 0.                                                                    |

## Input to `POST /collect`

The HTTP request body (`CollectEventDto`) is a subset — `eventId` is never accepted from
clients (always server-generated), and `timestamp`/`payload` are optional:

```jsonc
{
  "projectId": "game-1", // required, non-empty
  "type": "level_complete", // required, non-empty
  "timestamp": "2026-05-30T15:16:50.165Z", // optional ISO-8601
  "payload": { "level": 3 }, // optional object
}
```

Unknown properties are rejected (`ValidationPipe` `whitelist + forbidNonWhitelisted`).

## Example message on `raw-events`

```
key:   game-1
value: {"eventId":"8e8275f3-7874-43df-bbbf-f1a73a1aeb06","projectId":"game-1","type":"level_complete","timestamp":"2026-05-30T15:16:50.165Z","payload":{"level":3}}
```

## Cassandra mapping (Ingestion-Processor, KAN-18)

The Ingestion-Processor consumes `raw-events` and appends each event to
`cascade.raw_events`. The envelope maps to columns as:

| RawEvent field | Column        | Type        | Notes                                                                               |
| -------------- | ------------- | ----------- | ----------------------------------------------------------------------------------- |
| `projectId`    | `project_id`  | `text`      | Partition key (part 1).                                                             |
| _derived_      | `time_window` | `text`      | Partition key (part 2). Hourly UTC bucket `YYYY-MM-DDTHH` derived from `timestamp`. |
| `eventId`      | `event_id`    | `uuid`      | Clustering key → idempotent upsert.                                                 |
| `type`         | `type`        | `text`      |                                                                                     |
| `timestamp`    | `event_time`  | `timestamp` | Original event time.                                                                |
| `payload`      | `payload`     | `text`      | JSON-encoded.                                                                       |

- **Query served:** `SELECT * FROM cascade.raw_events WHERE project_id = ? AND time_window = ?`.
- **Idempotency:** the full primary key `((project_id, time_window), event_id)` makes a
  re-delivered event overwrite an identical row — safe under Kafka at-least-once.

## Phase notes

Phase 0 (KAN-17/18) does **light** validation and minimal modelling only. Per-project
schema validation, API-key auth, richer envelope fields, and secondary query tables are
Phase 1.
