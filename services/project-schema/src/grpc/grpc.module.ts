import { Module } from '@nestjs/common';
import { ApiKeysModule } from '../api-keys/api-keys.module';
import { SchemasModule } from '../schemas/schemas.module';
import { ProjectSchemaGrpcController } from './project-schema.grpc.controller';

/**
 * Wires the gRPC controller, reusing the existing API-key and schema services
 * (exported by their feature modules) so the sync contract shares logic with
 * the REST surface rather than duplicating it.
 */
@Module({
  imports: [ApiKeysModule, SchemasModule],
  controllers: [ProjectSchemaGrpcController],
})
export class GrpcModule {}
