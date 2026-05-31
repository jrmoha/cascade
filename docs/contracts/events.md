# Contract: `RawEvent` / `raw-events` topic

The canonical event envelope every Cascade event conforms to — the single shared data
contract across services. The **Collector** produces it onto the Kafka `raw-events` topic;
the **Ingestion-Processor** (and, later, the Aggregator) consume it.

**Single source of truth:** [`libs/contracts/src/events.ts`](../../libs/contracts/src/events.ts)
(`@cascade/contracts`) defines a **Zod** schema, `rawEventSchema`, and derives the TypeScript
type from it: `export type RawEvent = z.infer<typeof rawEventSchema>`. The static type and the
runtime validator therefore cannot drift. Producers validate before publishing; consumers
validate on receipt. The topic name is exported as `RAW_EVENTS_TOPIC = 'raw-events'`.

## Kafka message

| Aspect | Value                                                                 |
| ------ | --------------------------------------------------------------------- |
| Topic  | `raw-events`                                                          |
| Key    | `projectId` (UTF-8 string) — pins a project's events to one partition |
| Value  | JSON-serialized `RawEvent` (UTF-8)                                    |

## Envelope

| Field        | Type                | Required | Notes                                                                                                                                                      |
| ------------ | ------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `eventId`    | `string` (UUID)     | yes      | Server-stamped by the Collector. The downstream **idempotency key** for dedup (Cassandra clustering key — see ADR-0001).                                   |
| `projectId`  | `string`            | yes      | Tenant id; also the Kafka partition key.                                                                                                                   |
| `type`       | `string`            | yes      | Event type discriminator, e.g. `level_complete`.                                                                                                           |
| `occurredAt` | `string` (ISO-8601) | yes      | **Event time** — when it happened, reported by the client. Aggregation keys off this, not arrival order. Defaulted to `receivedAt` if the client omits it. |
| `receivedAt` | `string` (ISO-8601) | yes      | **Ingest time** — when the Collector accepted the event. Stamped server-side.                                                                              |
| `payload`    | `object`            | yes      | Arbitrary type-specific body; defaults to `{}`.                                                                                                            |
| `sessionId`  | `string`            | no       | Client session the event belongs to.                                                                                                                       |
| `actorId`    | `string`            | no       | The player/user the event is about.                                                                                                                        |
| `source`     | `string`            | no       | Emitting source / SDK version, e.g. `unity-sdk@1.4.0`.                                                                                                     |

The schema is **strict**: unknown keys are rejected.

### Event time vs ingest time

`occurredAt` (event time) and `receivedAt` (ingest time) are deliberately separate. Late and
out-of-order events are normal in telemetry — a client may buffer events offline and flush them
minutes later. Aggregations and the Cassandra `time_window` bucket key off `occurredAt`, so an
event lands in the window for **when it happened**, not when it arrived.

## Input to `POST /collect`

The HTTP request body (`CollectEventDto`) is a deliberate **subset** of the envelope, validated
at the HTTP boundary with `class-validator` (it is not a second source of truth — the Collector
builds the full envelope and validates it against `rawEventSchema` before producing). Clients
never supply `eventId` (server-generated) or `receivedAt` (ingest time); `occurredAt`, `payload`
and the optional fields may be omitted:

```jsonc
{
  "projectId": "game-1", // required, non-empty
  "type": "level_complete", // required, non-empty
  "occurredAt": "2026-05-30T15:16:50.165Z", // optional ISO-8601 event time
  "payload": { "level": 3 }, // optional object
  "sessionId": "sess-9", // optional
  "actorId": "player-42", // optional
  "source": "unity-sdk@1.4.0", // optional
}
```

Unknown properties are rejected (`ValidationPipe` `whitelist + forbidNonWhitelisted`).

## Example message on `raw-events`

```
key:   game-1
value: {"eventId":"8e8275f3-7874-43df-bbbf-f1a73a1aeb06","projectId":"game-1","type":"level_complete","occurredAt":"2026-05-30T15:16:50.165Z","receivedAt":"2026-05-30T15:16:50.200Z","payload":{"level":3},"sessionId":"sess-9","actorId":"player-42","source":"unity-sdk@1.4.0"}
```

## Cassandra mapping (Ingestion-Processor)

The Ingestion-Processor validates each consumed message against `rawEventSchema`, then appends it
to `cascade.raw_events`. The envelope maps to columns as:

| RawEvent field | Column        | Type        | Notes                                                                                |
| -------------- | ------------- | ----------- | ------------------------------------------------------------------------------------ |
| `projectId`    | `project_id`  | `text`      | Partition key (part 1).                                                              |
| _derived_      | `time_window` | `text`      | Partition key (part 2). Hourly UTC bucket `YYYY-MM-DDTHH` derived from `occurredAt`. |
| `eventId`      | `event_id`    | `uuid`      | Clustering key → idempotent upsert.                                                  |
| `type`         | `type`        | `text`      |                                                                                      |
| `occurredAt`   | `occurred_at` | `timestamp` | Event time.                                                                          |
| `receivedAt`   | `received_at` | `timestamp` | Ingest time.                                                                         |
| `payload`      | `payload`     | `text`      | JSON-encoded.                                                                        |
| `sessionId`    | `session_id`  | `text`      | Nullable.                                                                            |
| `actorId`      | `actor_id`    | `text`      | Nullable.                                                                            |
| `source`       | `source`      | `text`      | Nullable.                                                                            |

- **Query served:** `SELECT * FROM cascade.raw_events WHERE project_id = ? AND time_window = ?`.
- **Idempotency:** the full primary key `((project_id, time_window), event_id)` makes a
  re-delivered event overwrite an identical row — safe under Kafka at-least-once.

## Phase notes

The canonical envelope (KAN-21) is the contract going forward. Still **light** validation only:
per-project event-schema validation (validating `payload` per `type`), API-key auth, and a
dead-letter topic for invalid messages are later Phase 1 tickets. Honoring a client-supplied
`eventId` is a small ADR-0002 follow-up.
