# Contract: Project/Schema service

The bounded context that owns **projects, API keys, and per-project event schemas** in
PostgreSQL (KAN-28). It is the authoritative source for "is this API key valid?" and "what shape
should this project's `<eventType>` events have?" — the two questions the Collector asks on its
hot path (KAN-30). Low-volume, relational, consistency-sensitive config: the workload Postgres is
the right tool for (the opposite of Cassandra's write firehose).

**Two layers, kept separate (see [ADR-0011](../adr/0011-project-schema-service.md)):**

- **Wire/API contracts** — [`libs/contracts/src/project-schema.ts`](../../libs/contracts/src/project-schema.ts)
  (`@cascade/contracts`). Zod schemas are the single source of truth; the TypeScript types are
  derived via `z.infer`, so validator and type cannot drift. The Collector consumes the
  verify-key contract, which is why it lives in the shared package.
- **Persistence** — owned by **Prisma** (`services/project-schema/prisma/schema.prisma`).
  Versioned migrations under `prisma/migrations/` are the single source of truth for the DB and
  are applied by the service on boot (`prisma migrate deploy`) and via `npm run migrate`.

## Data model (PostgreSQL)

| Table           | Columns                                                                                                 | Notes                                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `projects`      | `id` (uuid, pk), `name`, `created_at`                                                                   | A tenant. Owns its keys and schemas (`ON DELETE CASCADE`).                                            |
| `api_keys`      | `id` (uuid, pk), `project_id` (fk), `prefix` (**unique**), `hash`, `created_at`, `revoked_at`           | Only the argon2 `hash` is stored — **never the plaintext secret**. `prefix` is the indexed lookup id. |
| `event_schemas` | `id` (uuid, pk), `project_id` (fk), `event_type`, `json_schema` (**jsonb**), `created_at`, `updated_at` | `@@unique(project_id, event_type)` — retrievable by that pair. JSON Schema stored as jsonb.           |

## API keys — format & verification

An issued key is `cas_<id8>.<secret32>`:

- `cas_<id8>` is the non-secret **prefix** — stored, unique-indexed, and returned in metadata.
- `<secret32>` is the **secret** — argon2-hashed into `api_keys.hash`; never stored or returned
  after issue.

**Verify is one cheap lookup, then a hash compare** (hot path): split the presented key on the
first `.`, look the row up by `prefix` (unique index), reject if `revoked_at` is set, then
`argon2.verify(hash, secret)`. A malformed, unknown, revoked, or mismatched key all return
`{ "valid": false }` without revealing which — verify never throws and never leaks.

## Operations (HTTP)

| Method & path                           | Purpose                          | Body / params                   | Response                                  |
| --------------------------------------- | -------------------------------- | ------------------------------- | ----------------------------------------- |
| `POST /projects`                        | Create a project                 | `{ "name": string }`            | `201` `Project`                           |
| `POST /projects/:id/keys`               | Issue an API key                 | —                               | `201` `IssuedApiKey` (plaintext **once**) |
| `POST /projects/:id/keys/:keyId/revoke` | Revoke a key                     | —                               | `200` `ApiKeyMetadata`                    |
| `POST /api-keys/verify`                 | Verify a key (hot path)          | `{ "key": string }`             | `200` `{ valid, projectId? }`             |
| `POST /projects/:id/schemas`            | Register/replace an event schema | `{ "eventType", "jsonSchema" }` | `201` `EventSchemaRecord`                 |
| `GET /projects/:id/schemas`             | List a project's schemas         | —                               | `200` `EventSchemaRecord[]`               |
| `GET /projects/:id/schemas/:eventType`  | Fetch one by `(projectId, type)` | —                               | `200` `EventSchemaRecord` / `404`         |

Request bodies are validated against the shared Zod schemas (`createProjectSchema`,
`verifyKeyRequestSchema`, `registerEventSchemaSchema`); a bad body returns a structured `400`
listing each failing field — the same shape the Collector uses.

### Examples

Create a project, then issue a key:

```jsonc
// POST /projects  → 201
{ "id": "9f1c…", "name": "Galaxy Raiders", "createdAt": "2026-06-01T15:00:00.000Z" }

// POST /projects/9f1c…/keys  → 201  (the only time `key` is ever returned)
{
  "id": "2a7b…",
  "projectId": "9f1c…",
  "prefix": "cas_a1b2c3d4",
  "createdAt": "2026-06-01T15:01:00.000Z",
  "revokedAt": null,
  "key": "cas_a1b2c3d4.Xy9…32chars"
}
```

Verify, register a schema, revoke:

```jsonc
// POST /api-keys/verify  { "key": "cas_a1b2c3d4.Xy9…" }  → 200
{ "valid": true, "projectId": "9f1c…" }

// POST /projects/9f1c…/schemas
{ "eventType": "level_complete",
  "jsonSchema": { "type": "object", "properties": { "level": { "type": "integer" } }, "required": ["level"] } }
// → 201 EventSchemaRecord (re-registering the same eventType upserts in place)

// POST /projects/9f1c…/keys/2a7b…/revoke  → 200  (revokedAt now set)
// subsequent POST /api-keys/verify with that key → { "valid": false }
```

## Synchronous contract (gRPC)

The internal hot-path call the Collector makes (KAN-30) is **gRPC**, not REST — the one justified
sync dependency in [ADR-0009](../adr/0009-service-boundaries-and-communication.md) §4. The contract
is [`libs/contracts/proto/project_schema.proto`](../../libs/contracts/proto/project_schema.proto),
ts-proto-generated into `libs/contracts/src/generated/` (committed; re-exported as the
`projectSchemaProto` namespace) via `npm run proto:gen`. Project/Schema is therefore a **hybrid HTTP +
gRPC app**: the REST table above is its admin surface; these RPCs are the service-to-service contract.
See [ADR-0012](../adr/0012-inter-service-contract-versioning.md).

| RPC (`package cascade.projectschema.v1`, service `ProjectSchema`) | Request                    | Response / errors                                                          |
| ----------------------------------------------------------------- | -------------------------- | -------------------------------------------------------------------------- |
| `VerifyKey`                                                       | `{ key }`                  | `{ valid, projectId? }` — invalid/revoked is data, not an error            |
| `GetEventSchema`                                                  | `{ projectId, eventType }` | `EventSchema` (`jsonSchema` is a **JSON string**); `NOT_FOUND` when absent |

Both RPCs delegate to the same `ApiKeysService` / `SchemasService` as the REST controllers, so there
is no duplicated logic. `jsonSchema` travels as a JSON-encoded string because proto3 has no
arbitrary-object type — callers `JSON.parse` it back. The gRPC bind address is `GRPC_URL`
(default `0.0.0.0:50051`). Compatibility rule: proto field numbers are part of the contract — never
reuse or renumber; add fields/RPCs additively (ADR-0012).

## Health

`GET /health` (liveness) and `GET /ready` (readiness, pings Postgres via `SELECT 1`) per
[ADR-0010](../adr/0010-independently-deployable-services.md). Container port **3004**; gRPC on
**50051**.

## Phase notes

KAN-28 built the service and its operations; KAN-29 added the typed gRPC sync contract (above);
**KAN-30 wired the caller** — the Collector now makes the `VerifyKey` / `GetEventSchema` calls on its
ingest hot path (per-request key verification + schema fetch, **Redis-cached** and **fail-closed**),
the single justified synchronous dependency recorded in
[ADR-0009](../adr/0009-service-boundaries-and-communication.md) §4 and detailed in
[ADR-0013](../adr/0013-collector-ingest-auth-validation-caching.md). Storing schemas as JSON Schema in
jsonb is what lets the Collector validate a project's events dynamically, with no redeploy when a new
event type is added.
