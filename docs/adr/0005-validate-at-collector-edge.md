# 0005 — Validate at the Collector edge with the shared contract

**Status:** Accepted

## Context

KAN-22 makes the Collector the **front gate**: every event is validated before it is produced to
`raw-events`, because once bad data is on the topic and in Cassandra it is expensive to undo. The
acceptance criteria require that validation use the shared contract from KAN-21 (not a
re-implemented copy), reject invalid events with a structured, per-field `400`, stamp `receivedAt`
server-side, and ignore a client-supplied `receivedAt`.

Until now the HTTP boundary used a hand-written `class-validator` DTO (`CollectEventDto`) that
re-listed the field rules — exactly the duplicated definition KAN-22 forbids. ADR-0004 had
deferred adopting a Zod pipe at the HTTP layer; KAN-22 is the ticket that needs it.

## Decision

1. **Validate the request body against a schema derived from `rawEventSchema`.**
   `collectEventSchema = rawEventSchema.omit({ eventId, receivedAt }).partial({ occurredAt }).strip()`
   lives in `@cascade/contracts`. It is _derived_, not duplicated, so the gate and the canonical
   envelope cannot drift. The Collector still runs the fully-assembled envelope through
   `rawEventSchema.parse` before producing (defence in depth).

2. **A small `ZodValidationPipe`** (in the Collector) parses the body with a given schema and, on
   failure, throws `400` with a structured body: `{ statusCode, error, message, errors: [{ field,
reason }] }` — one entry per failing field, so clients can act on it programmatically. This
   replaces the `class-validator` DTO, which is deleted.

3. **Keys the client does not own are stripped, not rejected** (`.strip()`). A client-supplied
   `receivedAt`/`eventId` (or any unknown field) is silently ignored; `receivedAt` is re-stamped
   server-side at acceptance. This satisfies the AC's "ignored/overwritten" wording and is
   forward-compatible for an ingest API, at the cost of no longer 400-ing on stray keys (the old
   `forbidNonWhitelisted` behaviour).

## Consequences

- One validation definition shared by the edge and the wire contract; no invalid event reaches
  `raw-events` (covered by tests, including the producer-not-called assertion).
- The Collector no longer depends on `class-validator` for `/collect`. The global `ValidationPipe`
  remains in place but is inert for this route.
- Stray/unknown request keys are silently dropped rather than rejected. If we later want strict
  rejection of unknown keys while still ignoring server-stamped ones, that is a localised change to
  `collectEventSchema`.
- Supersedes ADR-0004's deferral of a Collector-side Zod pipe.
