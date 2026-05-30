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

## Phase notes

Phase 0 (KAN-17) does **light** validation only. Per-project schema validation, API-key
auth, and richer envelope fields are Phase 1.
