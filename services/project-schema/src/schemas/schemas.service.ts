import { Injectable, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import type { EventSchemaRecord, RegisterEventSchemaInput } from '@cascade/contracts';
import { DatabaseService } from '../db/database.service';
import { ProjectsService } from '../projects/projects.service';
import { toEventSchemaDto } from '../common/mappers';

@Injectable()
export class SchemasService {
  constructor(
    private readonly db: DatabaseService,
    private readonly projects: ProjectsService,
  ) {}

  /**
   * Register (or replace) the JSON Schema for a project's event type. Upserts on
   * the unique `(projectId, eventType)` so re-registering an existing type
   * updates it in place — no duplicates, retrievable by that pair.
   */
  async register(projectId: string, input: RegisterEventSchemaInput): Promise<EventSchemaRecord> {
    await this.projects.assertExists(projectId);

    const jsonSchema = input.jsonSchema as Prisma.InputJsonObject;
    const row = await this.db.eventSchema.upsert({
      where: { projectId_eventType: { projectId, eventType: input.eventType } },
      create: { projectId, eventType: input.eventType, jsonSchema },
      update: { jsonSchema },
    });
    return toEventSchemaDto(row);
  }

  /** List all event schemas registered for a project. */
  async list(projectId: string): Promise<EventSchemaRecord[]> {
    await this.projects.assertExists(projectId);
    const rows = await this.db.eventSchema.findMany({
      where: { projectId },
      orderBy: { eventType: 'asc' },
    });
    return rows.map(toEventSchemaDto);
  }

  /** Fetch a single schema by `(projectId, eventType)`; `404` if absent. */
  async getByType(projectId: string, eventType: string): Promise<EventSchemaRecord> {
    const row = await this.db.eventSchema.findUnique({
      where: { projectId_eventType: { projectId, eventType } },
    });
    if (!row) {
      throw new NotFoundException(
        `No schema for event type "${eventType}" in project ${projectId}`,
      );
    }
    return toEventSchemaDto(row);
  }
}
