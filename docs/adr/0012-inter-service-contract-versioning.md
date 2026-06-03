# 0012 — Inter-service contracts & versioning strategy

**Status:** Accepted

## Context

[ADR-0009](0009-service-boundaries-and-communication.md) inventoried _which_ services talk and
_how_ (async Kafka by default; one justified sync call). It did not pin down how the messages and
calls on those seams are **defined, shared, and evolved**. As Phase 1 wires services together
(KAN-27→30), the risk is a breaking cross-service change — a renamed Kafka field, a dropped RPC
field — landing **silently** and failing in production rather than at build time.

Two contract surfaces exist:

- The **`raw-events` Kafka envelope** — already a shared Zod schema (`rawEventSchema`,
  [ADR-0004](0004-canonical-event-contract.md)) imported by the Collector (producer) and the
  Ingestion-Processor (consumer). But it carried no version field, so there was no way to evolve it
  deliberately or for a consumer to reason about which schema produced a message.
- The **one synchronous call** — Collector → Project/Schema (`VerifyKey` / `GetEventSchema`,
  [ADR-0009](0009-service-boundaries-and-communication.md) §4). The callee existed only as REST; the
  hot-path call had no typed, generated contract.

This ADR records how both are defined and versioned, and how an incompatible change is **enforced**
to fail CI (KAN-29).

## Decision

### 1. One shared definition per contract, in `@cascade/contracts`

Every cross-service shape lives once in `@cascade/contracts` and is imported by both sides — never
hand-copied. There are two complementary representations, by transport:

- **Async (Kafka) and REST wire shapes → Zod.** The Zod schema is the source of truth; the
  TypeScript type is `z.infer`'d from it so validator and type cannot drift (ADR-0004). This already
  covers `rawEventSchema` and the Project/Schema REST surface (`verifyKeyRequestSchema`, …).
- **Synchronous gRPC → a `.proto` + ts-proto.** The contract is
  [`libs/contracts/proto/project_schema.proto`](../../libs/contracts/proto/project_schema.proto);
  `npm run proto:gen` generates committed TypeScript interfaces into `src/generated/` (re-exported as
  the `projectSchemaProto` namespace). The generated type **is** the contract.

### 2. gRPC for the internal sync call (REST stays the admin surface)

The Collector→Project/Schema hot-path call is **gRPC** (`ProjectSchema.VerifyKey` /
`GetEventSchema`). Rationale: a `.proto` gives a single typed schema both sides generate from, with a
first-class NestJS transport and an explicit field-number compatibility model. Project/Schema becomes
a **hybrid app** — the existing REST API (project/key/schema administration) plus a gRPC microservice
for the two hot-path RPCs (same HTTP-plus-microservice shape as the Ingestion-Processor,
[ADR-0010](0010-independently-deployable-services.md)). The REST `/api-keys/verify` and schema-fetch
endpoints remain for operators/tooling; gRPC is the canonical service-to-service contract. The
Collector-side client is wired in KAN-30.

Serialization stays **JSON over Kafka** with an explicit version field (below) — the simple,
senior-credible default. Avro/Protobuf **with a schema registry** (the Confluent/MSK pattern) is the
"real" path at scale and is deliberately deferred.

### 3. Versioning & compatibility convention

The rule, everywhere: **additive is safe; rename / remove / retype is breaking.** Consumers tolerate
unknown fields (forward-compatible) and never depend on field order.

- **Kafka envelope:** carries an integer `schemaVersion` (`RAW_EVENT_SCHEMA_VERSION`, currently `1`),
  server-stamped by the Collector. Leave it untouched for additive changes; bump it only for a
  breaking change, alongside a documented migration. `.default(1)` keeps a versionless legacy message
  parseable.
- **gRPC/proto:** field numbers are part of the contract — never reuse or renumber a field, never
  change a field's type. Add new fields (fresh numbers) and new RPCs additively.

### 4. Enforcement — a breaking change fails CI

Documentation is not enough; the convention is enforced by [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml):

- **Contract snapshot** (`libs/contracts/test/contract-snapshot.spec.ts`) serializes every shared
  Zod contract to JSON Schema and snapshots it. _Any_ change to the wire surface fails the test until
  the snapshot is regenerated (`vitest -u`) — the checkpoint at which the additive-vs-breaking call
  is made and a version bumped if needed.
- **`proto:check`** regenerates ts-proto and `git diff --exit-code`s `src/generated`: a proto edit
  without regeneration, or any drift, fails.
- **`tsc --build`** typechecks the whole project-reference graph: a removed/renamed contract field
  breaks every consumer that references it.

Docker/Testcontainers integration and the smoke gate run locally (`SKIP_INTEGRATION=1` in CI) to
keep the gate fast and deterministic. This is the seed of Phase 6's consumer-contract testing (Pact).

## Alternatives considered

- **REST + shared Zod for the sync call (no gRPC).** Smaller change — the Zod request/response
  shapes already existed. Rejected for the internal call: the ticket targets a generated, typed RPC
  contract, and `.proto` field numbers give an explicit compatibility model. REST is retained for the
  admin surface, so nothing is lost.
- **Avro/Protobuf + a schema registry now.** The production-grade path, but heavy for Phase 1 and
  premature with a single producer. Named here as the deferred target rather than adopted.
- **Documentation-only convention.** Rejected — the criterion that matters is _enforcement_; a rule
  no build checks is a rule that drifts.

## Consequences

**Positive:**

- One shared definition per contract; both sides import it, so types cannot drift.
- A breaking change fails CI (snapshot, `proto:check`, or `tsc`) instead of production.
- The additive/breaking rule and `schemaVersion` give a deliberate path to evolve the envelope.

**Trade-offs:**

- A `.proto` plus a generated, committed artifact adds a codegen step (`proto:gen`) and a CI drift
  check; the generated dir is lint/format-ignored to keep it byte-stable.
- Project/Schema now runs two transports (HTTP + gRPC) and binds a second port.
- The snapshot tripwire fires on _every_ contract change, additive included — intentional friction
  that forces the compatibility judgement, but it means additive changes still touch the snapshot.
