# Runbook: Dead-letter queue (`raw-events.dlq`)

Events that fail **downstream** processing in the Ingestion-Processor are routed to the
`raw-events.dlq` topic instead of being dropped or blocking the partition (KAN-23, ADR-0006). This
runbook covers what lands there, how to inspect it, and how to replay.

## What gets dead-lettered

| `error.kind`  | Cause                                                               | Retried first?                          |
| ------------- | ------------------------------------------------------------------- | --------------------------------------- |
| `validation`  | Message can't be deserialized / fails the `rawEventSchema` contract | No — permanent, dead-lettered at once   |
| `persistence` | Valid event whose Cassandra write kept failing                      | Yes — 3 attempts, backoff 200ms → 400ms |

Edge-rejected input (bad `POST /collect`) never reaches Kafka at all — that's KAN-22, a `400`, not a
dead-letter.

## DLQ message shape

A `DeadLetter` (see `@cascade/contracts` `deadLetterSchema`):

```jsonc
{
  "originalValue": "…", // the raw Kafka value, verbatim — replay loses nothing
  "originalEvent": {
    /* parsed RawEvent, present only for persistence failures */
  },
  "error": { "kind": "validation", "reason": "Expected object, received string" },
  "attempts": 1,
  "failedAt": "2026-05-31T00:00:00.000Z",
  "source": { "topic": "raw-events", "partition": 0, "offset": "42", "key": "game-1" },
}
```

## Inspect

Local stack — browse in **kafka-ui** at <http://localhost:8080> (topic `raw-events.dlq`), or from the
CLI:

```bash
# Count / read DLQ messages from the beginning
docker exec cascade-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw-events.dlq --from-beginning --timeout-ms 5000

# How many are sitting in the DLQ (per-partition end offsets)
docker exec cascade-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic raw-events.dlq
```

Triage by `error.kind`: a spike of `persistence` points at Cassandra (down, overloaded, schema
drift); `validation` points at a producer emitting events that bypass or violate the contract.

## Replay

Once the root cause is fixed, re-emit the originals onto `raw-events`. Writes are idempotent
(primary key `((project_id, time_window), event_id)`), so replaying an event that _did_ eventually
land is harmless.

- **Persistence failures** carry a parsed `originalEvent` — re-publish that object to `raw-events`,
  keyed by its `projectId`.
- **Validation failures** carry only `originalValue` (no valid event) — these need the producer
  fixed; only replay once the value is corrected.

A minimal manual replay (persistence failures): consume `raw-events.dlq`, and for each record with
`error.kind == "persistence"`, produce `originalEvent` back to `raw-events` with `key =
originalEvent.projectId`. An automated replay tool is a future ticket; for now this is a deliberate,
supervised operation.

## Guarantees

- A poison message never crashes the consumer or blocks its partition — processing continues past it
  (covered by `services/ingestion-processor/test/dlq.e2e-spec.ts`).
- Nothing is silently lost: every failure is in `raw-events.dlq` with its reason, attempt count, and
  source offset for inspection and replay.
