import { Module } from '@nestjs/common';
import { ProjectsModule } from '../projects/projects.module';
import { SchemasController } from './schemas.controller';
import { SchemasService } from './schemas.service';

@Module({
  imports: [ProjectsModule],
  controllers: [SchemasController],
  providers: [SchemasService],
  exports: [SchemasService],
})
export class SchemasModule {}
