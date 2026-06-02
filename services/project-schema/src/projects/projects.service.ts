import { Injectable, NotFoundException } from '@nestjs/common';
import type { CreateProjectInput, Project } from '@cascade/contracts';
import { DatabaseService } from '../db/database.service';
import { toProjectDto } from '../common/mappers';

@Injectable()
export class ProjectsService {
  constructor(private readonly db: DatabaseService) {}

  /** Create a project (tenant). */
  async create(input: CreateProjectInput): Promise<Project> {
    const row = await this.db.project.create({ data: { name: input.name } });
    return toProjectDto(row);
  }

  /**
   * Assert a project exists, throwing `404` otherwise. Used by the key/schema
   * flows so a bad `projectId` is a clear not-found rather than an opaque FK
   * violation.
   */
  async assertExists(projectId: string): Promise<void> {
    const row = await this.db.project.findUnique({ where: { id: projectId } });
    if (!row) {
      throw new NotFoundException(`Project ${projectId} not found`);
    }
  }
}
