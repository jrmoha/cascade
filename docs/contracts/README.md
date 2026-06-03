# Contracts

The shared contracts that define every cross-service boundary in Cascade. All of them live in
`@cascade/contracts` ([`libs/contracts`](../../libs/contracts)) as a **single source of truth** and
are imported by both sides — never hand-copied. This page indexes them; the per-contract pages hold
the detail.

## How contracts are represented

| Transport                 | Representation                  | Source of truth                                                               |
| ------------------------- | ------------------------------- | ----------------------------------------------------------------------------- |
| Kafka (async) & REST wire | **Zod** schema → `z.infer` type | `libs/contracts/src/*.ts`                                                     |
| Synchronous service call  | **gRPC `.proto`** → ts-proto    | `libs/contracts/proto/*.proto` → `libs/contracts/src/generated/*` (committed) |

Zod gives one schema that is both the runtime validator and the static type, so they cannot drift
(ADR-0004). For the gRPC call, the ts-proto-generated TypeScript **is** the contract; regenerate with
`npm run proto:gen`.

## Index

| Contract                               | Surface                                  | Spec / source                                                                                                                                                                                                       |
| -------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`RawEvent` / `raw-events`](events.md) | Kafka envelope (Collector → Processor)   | [`events.ts`](../../libs/contracts/src/events.ts) · [collector.openapi.yaml](../specs/collector.openapi.yaml)                                                                                                       |
| [Project/Schema](project-schema.md)    | REST admin + **gRPC** sync (→ Collector) | [`project-schema.ts`](../../libs/contracts/src/project-schema.ts) · [`project_schema.proto`](../../libs/contracts/proto/project_schema.proto) · [project-schema.openapi.yaml](../specs/project-schema.openapi.yaml) |
| Query API (raw read-back)              | REST read path                           | [query-api.openapi.yaml](../specs/query-api.openapi.yaml)                                                                                                                                                           |
| Dead letters (`*.dlq`)                 | Kafka DLQ envelope                       | [`dead-letter.ts`](../../libs/contracts/src/dead-letter.ts)                                                                                                                                                         |

OpenAPI specs for the HTTP/edge surfaces live under [`docs/specs/`](../specs/).

## Versioning & compatibility

The rule everywhere: **additive is safe; rename / remove / retype is breaking.** Consumers tolerate
unknown fields and never depend on field order.

- **Kafka envelope:** carries a server-stamped integer `schemaVersion` (`RAW_EVENT_SCHEMA_VERSION`).
  Bump only for a breaking change; additive changes leave it untouched.
- **gRPC/proto:** field numbers are part of the contract — never reuse or renumber; add fields/RPCs
  additively.

**Enforcement.** A breaking change fails CI ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)),
not production:

- the JSON-Schema **contract snapshot** test ([`contract-snapshot.spec.ts`](../../libs/contracts/test/contract-snapshot.spec.ts)) — regenerate intentionally with `vitest -u`;
- **`npm run proto:check`** — regenerate ts-proto and `git diff --exit-code` the committed output;
- **`tsc --build`** — a removed/renamed field breaks every consumer that references it.

See [ADR-0012](../adr/0012-inter-service-contract-versioning.md) for the full rationale, and
[ADR-0009](../adr/0009-service-boundaries-and-communication.md) for the topic and sync-call
inventories.
