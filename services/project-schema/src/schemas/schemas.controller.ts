import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
} from '@nestjs/common';
import {
  type EventSchemaRecord,
  type RegisterEventSchemaInput,
  registerEventSchemaSchema,
} from '@cascade/contracts';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { SchemasService } from './schemas.service';

@Controller('projects/:projectId/schemas')
export class SchemasController {
  constructor(private readonly schemas: SchemasService) {}

  /** Register (or replace) the schema for an event type. */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  register(
    @Param('projectId', ParseUUIDPipe) projectId: string,
    @Body(new ZodValidationPipe(registerEventSchemaSchema)) input: RegisterEventSchemaInput,
  ): Promise<EventSchemaRecord> {
    return this.schemas.register(projectId, input);
  }

  /** List a project's registered event schemas. */
  @Get()
  list(@Param('projectId', ParseUUIDPipe) projectId: string): Promise<EventSchemaRecord[]> {
    return this.schemas.list(projectId);
  }

  /** Fetch one schema by event type. */
  @Get(':eventType')
  getByType(
    @Param('projectId', ParseUUIDPipe) projectId: string,
    @Param('eventType') eventType: string,
  ): Promise<EventSchemaRecord> {
    return this.schemas.getByType(projectId, eventType);
  }
}
