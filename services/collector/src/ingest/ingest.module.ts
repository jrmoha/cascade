import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { PROJECT_SCHEMA_PROTO_PACKAGE, PROJECT_SCHEMA_PROTO_PATH } from '@cascade/contracts';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';
import { ApiKeyGuard } from './api-key.guard';
import { PROJECT_SCHEMA_CLIENT } from './ingest.tokens';
import { ProjectSchemaClient } from './project-schema.client';
import { RateLimiter } from './rate-limit';
import { RateLimitGuard } from './rate-limit.guard';

/**
 * The ingest auth + validation slice (KAN-30): a gRPC client to Project/Schema
 * plus the {@link ProjectSchemaClient} accessor and the {@link ApiKeyGuard} that
 * use it. The Kafka producer stays in {@link CollectorModule}; this module owns
 * only the synchronous Project/Schema dependency. Relies on the global
 * {@link RedisModule} for the cache connection.
 */
@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: PROJECT_SCHEMA_CLIENT,
        inject: [APP_CONFIG],
        useFactory: (config: CollectorConfig) => ({
          transport: Transport.GRPC,
          options: {
            package: PROJECT_SCHEMA_PROTO_PACKAGE,
            protoPath: PROJECT_SCHEMA_PROTO_PATH,
            url: config.PROJECT_SCHEMA_GRPC_URL,
          },
        }),
      },
    ]),
  ],
  providers: [ProjectSchemaClient, ApiKeyGuard, RateLimiter, RateLimitGuard],
  // Export the guards AND their injected deps: a controller-scoped guard is
  // instantiated in the consuming module's context (CollectorModule), so its
  // dependencies must be visible there too (mirrors ProjectSchemaClient for
  // ApiKeyGuard).
  exports: [ProjectSchemaClient, ApiKeyGuard, RateLimiter, RateLimitGuard],
})
export class IngestModule {}
