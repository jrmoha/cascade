# 0004 — Canonical event contract (Zod single-source envelope; event time vs ingest time)

**Status:** Accepted

## Context

KAN-21 opens Phase 1. The shared event envelope (`RawEvent`) already lived in
`@cascade/contracts` and was imported by every service, but it had two weaknesses:

1. It was a hand-written TypeScript `interface` with **no runtime validator**. Each boundary
   (Collector HTTP input, Kafka producer, Kafka consumer) trusted the shape independently, so a
   malformed event could reach Cassandra. Any validator added separately could drift from the type.
2. It carried a single `timestamp`, conflating **event time** (when the event happened, per the
   client) with **ingest time** (when the Collector accepted it). Telemetry is routinely late and
   out-of-order; aggregation must key off event time, not arrival order, so the two must be distinct.

We needed one definition that yields _both_ the type and the validator, imported by Collector and
Processor, with the event-time/ingest-time split made explicit.

## Decision

1. **Zod is the single source of truth.** `rawEventSchema` (a Zod schema) is defined in
   `libs/contracts/src/events.ts`, and the TypeScript type is derived from it via
   `type RawEvent = z.infer<typeof rawEventSchema>`. Type and validator cannot drift. The schema is
   `.strict()` (unknown keys rejected). The Collector validates the envelope before producing; the
   Ingestion-Processor validates each consumed message before persisting.

2. **Split `occurredAt` (event time) from `receivedAt` (ingest time).** `occurredAt` comes from the
   client (defaulting to ingest time when omitted); `receivedAt` is stamped by the Collector. The
   Cassandra `time_window` partition bucket is derived from `occurredAt`, so events land in the
   window for when they happened.

3. **`eventId` is a required UUID** and is documented as the downstream idempotency key (the
   Cassandra clustering key — ADR-0001).

4. **Optional fields `sessionId`, `actorId`, `source`** are part of the envelope and are persisted
   end-to-end (nullable Cassandra columns), so the contract is complete with no silent drop.

5. **The Collector's HTTP input DTO stays `class-validator`.** `CollectEventDto` validates the HTTP
   boundary (returning 400s) as a documented _subset_ of the envelope. It is not a second source of
   truth: the Collector constructs the full envelope and runs it through `rawEventSchema` before
   producing.

Alternatives rejected:

- **Keep the hand-written interface + add a separate validator** (e.g. Ajv against a duplicated
  JSON Schema): reintroduces the drift risk this ticket exists to remove.
- **Make `class-validator` / the DTO the envelope source:** class-validator validates class
  instances, not arbitrary parsed Kafka payloads, and would not give the consumer a clean
  parse-and-type result. It fits the HTTP boundary, not the wire contract.
- **JSON Schema + codegen:** more portable to non-TS producers, but adds a codegen build step and a
  second toolchain for no current benefit — every producer/consumer today is TypeScript. Revisit if
  a non-TS producer appears.
- **A NestJS Zod validation pipe replacing the HTTP DTO:** viable, but mixes a new validation style
  into the HTTP layer for marginal gain; deferred at the time of this ADR. (Subsequently adopted in
  ADR-0005 / KAN-22, which needs the edge to validate against the shared contract.)

## Consequences

- One contract, one validator, no drift; invalid events are rejected at the Collector and skipped at
  the Processor (logged and dropped for now — a dead-letter topic is a later ticket, so a poison
  message under at-least-once delivery is not redelivered forever).
- The Cassandra `raw_events` table gains `occurred_at`, `received_at`, and nullable `session_id`,
  `actor_id`, `source` columns (replacing the old single `event_time`). `CREATE TABLE IF NOT EXISTS`
  does **not** alter a pre-existing local table, so a developer with persistent dev data must
  recreate it once (`docker compose -f infra/docker-compose.yml down -v`). Tests use ephemeral
  containers and are unaffected.
- `@cascade/contracts` now depends on `zod`. The dependency is shared by all services that import
  the contract.
- Per-project `payload` schema validation, API-key auth, and a DLQ remain out of scope (later Phase
  1 tickets).
