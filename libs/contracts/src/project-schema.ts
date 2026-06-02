import { z } from 'zod';

/**
 * Wire contracts for the Project/Schema service (KAN-28) — projects, API keys,
 * and per-project event schemas. As with the event envelope, these Zod schemas
 * are the **single source of truth**; TypeScript types are derived via
 * `z.infer` so the static type and the runtime validator can never drift.
 *
 * These describe the **wire surface only** (request/response shapes). The
 * relational persistence model is owned by Prisma (`prisma/schema.prisma`); the
 * two layers are intentionally separate (see ADR-0011). The Collector consumes
 * the verify-key contract on its hot path (KAN-30), which is why it lives in the
 * shared package rather than inside the service.
 */

/** Human-facing prefix on every issued key, e.g. `cas_a1b2c3d4`. */
export const API_KEY_PREFIX = 'cas_';

/** Length of the random, non-secret key id embedded after {@link API_KEY_PREFIX}. */
export const API_KEY_ID_LENGTH = 8;

/** Length of the random secret half of an issued key. */
export const API_KEY_SECRET_LENGTH = 32;

/** A registered project (tenant). */
export const projectSchema = z.object({
  id: z.string().uuid(),
  name: z.string().min(1),
  createdAt: z.string().datetime({ offset: true }),
});
export type Project = z.infer<typeof projectSchema>;

/** Input to create a project. */
export const createProjectSchema = z
  .object({
    name: z.string().min(1).max(200),
  })
  .strict();
export type CreateProjectInput = z.infer<typeof createProjectSchema>;

/**
 * Non-secret view of an API key. Never exposes the stored hash or the secret —
 * only the `prefix` (the indexed lookup id) and lifecycle timestamps.
 */
export const apiKeyMetadataSchema = z.object({
  id: z.string().uuid(),
  projectId: z.string().uuid(),
  prefix: z.string().min(1),
  createdAt: z.string().datetime({ offset: true }),
  revokedAt: z.string().datetime({ offset: true }).nullable(),
});
export type ApiKeyMetadata = z.infer<typeof apiKeyMetadataSchema>;

/**
 * Response when a key is issued. Carries the **one-time plaintext** `key`
 * (`cas_<prefix>.<secret>`) — it is never retrievable again, since only its
 * hash is stored.
 */
export const issuedApiKeySchema = apiKeyMetadataSchema.extend({
  key: z.string().min(1),
});
export type IssuedApiKey = z.infer<typeof issuedApiKeySchema>;

/** Hot-path request: verify a presented key (Collector → Project/Schema). */
export const verifyKeyRequestSchema = z
  .object({
    key: z.string().min(1),
  })
  .strict();
export type VerifyKeyRequest = z.infer<typeof verifyKeyRequestSchema>;

/** Hot-path response: whether the key is valid and, if so, its owning project. */
export const verifyKeyResponseSchema = z.object({
  valid: z.boolean(),
  projectId: z.string().uuid().optional(),
});
export type VerifyKeyResponse = z.infer<typeof verifyKeyResponseSchema>;

/**
 * A registered event schema. `jsonSchema` is an arbitrary JSON Schema document
 * (stored as jsonb) the Collector fetches to validate a project's events
 * dynamically, with no redeploy when a new event type is added (KAN-30).
 */
export const eventSchemaSchema = z.object({
  id: z.string().uuid(),
  projectId: z.string().uuid(),
  eventType: z.string().min(1),
  jsonSchema: z.record(z.unknown()),
  createdAt: z.string().datetime({ offset: true }),
  updatedAt: z.string().datetime({ offset: true }),
});
export type EventSchemaRecord = z.infer<typeof eventSchemaSchema>;

/** Input to register (or replace) the schema for an event type. */
export const registerEventSchemaSchema = z
  .object({
    eventType: z.string().min(1).max(200),
    jsonSchema: z.record(z.unknown()),
  })
  .strict();
export type RegisterEventSchemaInput = z.infer<typeof registerEventSchemaSchema>;
