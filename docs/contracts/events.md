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
minutes later. Aggregations and the Cassandra `time_bucket` partition key off `occurredAt`, so an
event lands in the window for **when it happened**, not when it arrived.

## Input to `POST /collect`

The request body is validated at the edge (KAN-22) against `collectEventSchema`, which is
**derived from `rawEventSchema`** (`rawEventSchema.omit({ eventId, receivedAt }).partial({ occurredAt }).strip()`)
— not a re-implemented copy, so the gate and the canonical contract can never diverge. Bad data
is rejected here and never reaches the `raw-events` topic. Clients never supply `eventId`
(server-generated) or `receivedAt` (ingest time); `occurredAt`, `payload`, and the optional
fields may be omitted:

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

- **Keys the client does not own are stripped, not rejected.** A client-supplied `receivedAt` or
  `eventId` (or any unknown field) is silently ignored; `receivedAt` is always re-stamped
  server-side at acceptance.
- **A missing or wrong-typed required field returns `400`** with a structured body listing each
  failing field and why:

  ```jsonc
  {
    "statusCode": 400,
    "error": "Bad Request",
    "message": "Event validation failed",
    "errors": [{ "field": "projectId", "reason": "Required" }],
  }
  ```

- A valid event returns `202 Accepted` with the server-stamped `eventId`.

This is synchronous, edge-level rejection. Downstream processing failures are a separate concern
(the dead-letter flow, KAN-23).

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
| _derived_      | `time_bucket` | `text`      | Partition key (part 2). Hourly UTC bucket `YYYY-MM-DDTHH` derived from `occurredAt`. |
| `occurredAt`   | `occurred_at` | `timestamp` | Event time. Clustering key (DESC) → newest-first reads.                              |
| `eventId`      | `event_id`    | `uuid`      | Clustering key (after `occurred_at`) → tie-break + uniqueness → idempotent upsert.   |
| `type`         | `type`        | `text`      |                                                                                      |
| `receivedAt`   | `received_at` | `timestamp` | Ingest time.                                                                         |
| `payload`      | `payload`     | `text`      | JSON-encoded.                                                                        |
| `sessionId`    | `session_id`  | `text`      | Nullable.                                                                            |
| `actorId`      | `actor_id`    | `text`      | Nullable.                                                                            |
| `source`       | `source`      | `text`      | Nullable.                                                                            |

- **Primary key:** `((project_id, time_bucket), occurred_at, event_id)` with
  `CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)` and a 30-day TTL. See **ADR-0007** for the
  partition-key/bucketing/TTL rationale.
- **Query served:** `SELECT … WHERE project_id = ? AND time_bucket = ?` — newest-first from the
  clustering order, never `ALLOW FILTERING`.
- **Idempotency:** the full primary key makes a re-delivered event overwrite an identical row — safe
  under Kafka at-least-once.
- **Schema source of truth:** the versioned migrations in
  `services/ingestion-processor/migrations/`, applied by the `Migrator` (on startup and via
  `npm run migrate`). Not created ad-hoc.

## Phase notes

The canonical envelope (KAN-21) is the contract going forward. Still **light** validation only:
per-project event-schema validation (validating `payload` per `type`), API-key auth, and a
dead-letter topic for invalid messages are later Phase 1 tickets. Honoring a client-supplied
`eventId` is a small ADR-0002 follow-up.
