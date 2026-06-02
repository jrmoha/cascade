import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { type CreateProjectInput, createProjectSchema, type Project } from '@cascade/contracts';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { ProjectsService } from './projects.service';

@Controller('projects')
export class ProjectsController {
  constructor(private readonly projects: ProjectsService) {}

  /** Register a new project. Returns the created project (incl. its id). */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  create(
    @Body(new ZodValidationPipe(createProjectSchema)) input: CreateProjectInput,
  ): Promise<Project> {
    return this.projects.create(input);
  }
}
