import 'reflect-metadata';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { PROJECT_SCHEMA_PROTO_PACKAGE, PROJECT_SCHEMA_PROTO_PATH } from '@cascade/contracts';
import { AppModule } from './app.module';
import { APP_CONFIG } from './config/config.module';
import type { ProjectSchemaConfig } from './config/env.schema';

/**
 * Hybrid app: the REST admin API + `/health`/`/ready` probes over HTTP, plus a
 * gRPC microservice serving the internal sync contract the Collector calls on
 * its ingest hot path (KAN-29/30). Same hybrid pattern as the Ingestion-Processor
 * (HTTP + a connected microservice).
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  const config = app.get<ProjectSchemaConfig>(APP_CONFIG);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.GRPC,
    options: {
      package: PROJECT_SCHEMA_PROTO_PACKAGE,
      protoPath: PROJECT_SCHEMA_PROTO_PATH,
      url: config.GRPC_URL,
    },
  });

  await app.startAllMicroservices();
  await app.listen(config.PORT);
  Logger.log(
    `Project/Schema service: HTTP on http://localhost:${config.PORT}, gRPC on ${config.GRPC_URL}`,
    'Bootstrap',
  );
}

void bootstrap();
