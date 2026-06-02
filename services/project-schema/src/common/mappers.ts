import type { ApiKey, EventSchema, Project } from '@prisma/client';
import type { ApiKeyMetadata, EventSchemaRecord, Project as ProjectDto } from '@cascade/contracts';

/**
 * Row → wire-contract mappers. Prisma returns `Date`s and the persistence
 * model; the contracts use ISO-8601 strings. Centralising the conversion keeps
 * the (intentional) DB/wire separation explicit and avoids leaking secret
 * columns (`api_keys.hash` is never mapped out).
 */

export function toProjectDto(row: Project): ProjectDto {
  return {
    id: row.id,
    name: row.name,
    createdAt: row.createdAt.toISOString(),
  };
}

export function toApiKeyMetadata(row: ApiKey): ApiKeyMetadata {
  return {
    id: row.id,
    projectId: row.projectId,
    prefix: row.prefix,
    createdAt: row.createdAt.toISOString(),
    revokedAt: row.revokedAt ? row.revokedAt.toISOString() : null,
  };
}

export function toEventSchemaDto(row: EventSchema): EventSchemaRecord {
  return {
    id: row.id,
    projectId: row.projectId,
    eventType: row.eventType,
    jsonSchema: row.jsonSchema as Record<string, unknown>,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}
