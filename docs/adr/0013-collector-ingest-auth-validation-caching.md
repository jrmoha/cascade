# 0013 — Collector ingest authentication, per-project schema validation & caching

**Status:** Accepted

## Context

[ADR-0009](0009-service-boundaries-and-communication.md) §4 named the one justified synchronous
dependency in Cascade: the Collector asking Project/Schema "is this API key valid?" and "what shape
should this event have?" at ingest time. [ADR-0011](0011-project-schema-service.md) built the callee
and [ADR-0012](0012-inter-service-contract-versioning.md) defined the gRPC contract
(`VerifyKey` / `GetEventSchema`). KAN-30 wires the **caller**: the Collector must now authenticate
every `POST /collect` and validate its payload against the project's registered JSON Schema, **without**
pinning ingest throughput to Postgres (a per-event lookup would do exactly that).

Three decisions had to be made deliberately: where `projectId` comes from once a key authenticates a
request; what happens when Project/Schema is unavailable; and how to cache without serving dangerously
stale answers.

## Decision

### 1. The API key authenticates **and** identifies the project

`POST /collect` requires an `x-api-key` header. An `ApiKeyGuard` resolves the owning `projectId` from
the key (via Project/Schema, cached) and attaches it to the request; the rest of the pipeline trusts
that server-derived value. **`projectId` is dropped from the request body** — a key can only ever
write to its own project, so a client cannot spoof another tenant's id. The shared `collectEventSchema`
therefore omits `projectId` (it joins `eventId`/`receivedAt`/`schemaVersion` as server-supplied); a
client that still sends one has it silently stripped (`.strip()`), keeping the change
backward-tolerant. The wire envelope `rawEventSchema` is unchanged — it still carries `projectId`,
now stamped by the Collector from the key.

### 2. Checks run cheapest-and-most-decisive first

1. **API key** (`ApiKeyGuard`) — missing/unknown/revoked → `401`.
2. **Envelope** (`collectEventSchema`, KAN-22) — a malformed envelope → structured `400`.
3. **Per-project schema** (Ajv) — an **unregistered** event type → `422`; a payload that violates the
   registered JSON Schema → structured `400` (same `{ message, errors:[{field,reason}] }` shape as the
   envelope error). Both the key and schema checks must pass before anything is produced to Kafka.

### 3. Redis cache + an in-process compiled-validator cache

- `apiKey → projectId` and the per-`(projectId, eventType)` JSON Schema are cached in **Redis** with a
  short TTL (`PROJECT_SCHEMA_CACHE_TTL_SECONDS`, default **30s**). A cache **hit makes no gRPC call**.
  A short TTL means a revoked key or an edited schema propagates within seconds without a per-event
  lookup. Known-bad answers (invalid key, unregistered type) are **negatively cached** too, so a flood
  of bad keys can't hammer Project/Schema.
- The API key is **SHA-256 hashed before it is used as a Redis key**, so plaintext secrets never land
  in the cache.
- Compiled Ajv `ValidateFunction`s can't be serialized, so Redis holds the **raw** schema and an
  in-process `Map` holds the **compiled** function, keyed by the schema's `updatedAt` (an edit
  recompiles). Ajv compiles each schema **once** — compiling per request is a silent throughput killer.

### 4. Fail-closed when Project/Schema is unavailable

When an answer is **neither cached nor obtainable** (Project/Schema unreachable, or a call times out),
the Collector **rejects** with `503` rather than admitting an unauthenticated or unvalidated event.
Cache hits are always served, so a Project/Schema outage degrades gracefully for already-seen keys and
schemas within the TTL window, but a cold miss never falls open. Redis is a Collector **readiness**
dependency (`GET /ready` pings it); Project/Schema is **not** — the cache + fail-closed path handle its
absence per-request.

## Alternatives considered

### Cached-allow (fail-open)

On a cold miss with Project/Schema down, admit the event (skip validation) and log. **Rejected.** It
favours availability over correctness on the _write_ path: it would accept events against an unknown
key or an unverified schema, exactly the thing this ticket exists to prevent. Fail-closed is the safer
default for ingest; a future ticket can revisit per-tenant policy. (This is the consistency-vs-
availability theme that returns in Phase 4.)

### `projectId` in the body, validated against the key

Keep the client-supplied `projectId` and reject a mismatch. **Rejected** as redundant and more
error-prone — deriving it from the key removes a whole class of spoofing and "wrong project" bugs.

### Per-event synchronous call (no cache)

**Rejected outright** — it pins ingest throughput to Postgres and adds a network round-trip to every
event. The cache is the whole point.

### Schema registry / Avro

Out of scope; named as the deferred "real" path in [ADR-0012](0012-inter-service-contract-versioning.md).

## Consequences

**Positive:**

- Only authenticated, well-formed events from a known project enter the pipeline; `projectId` is
  unspoofable.
- The hot path is a Redis lookup (one `GET`) on the common case; Project/Schema is touched only on a
  cold miss or after the TTL lapses.
- Fail-closed makes the failure mode explicit and safe, and it is covered by tests.
- Closes the Phase-2 loop: a real multi-service request now flows
  Collector → (Project/Schema, cached) → Kafka → Processor → Cassandra (verified by the e2e smoke).

**Trade-offs:**

- A revoked key or edited schema can still be honoured for up to one TTL (30s) — acceptable for the
  cache-vs-freshness trade; eager invalidation (a push from Project/Schema) is a possible follow-up.
- Redis is now on the ingest critical path (a readiness dependency).
- A Project/Schema outage stalls ingestion for **cold** keys/schemas (the deliberate fail-closed cost).
- Caching adds a small surface (cache poisoning is bounded — keys are hashed, values are project ids /
  schemas with a short TTL).
