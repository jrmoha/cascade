import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { DatabaseModule } from './db/database.module';
import { ProjectsModule } from './projects/projects.module';
import { ApiKeysModule } from './api-keys/api-keys.module';
import { SchemasModule } from './schemas/schemas.module';
import { HealthModule } from './health/health.module';
import { GrpcModule } from './grpc/grpc.module';

@Module({
  imports: [
    AppConfigModule,
    DatabaseModule,
    ProjectsModule,
    ApiKeysModule,
    SchemasModule,
    HealthModule,
    GrpcModule,
  ],
})
export class AppModule {}
