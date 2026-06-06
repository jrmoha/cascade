import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { APP_CONFIG } from './config/config.module';
import type { AggregatorConfig } from './config/env.schema';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  const config = app.get<AggregatorConfig>(APP_CONFIG);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'cascade-aggregator',
        brokers: config.KAFKA_BOOTSTRAP_SERVERS,
      },
      consumer: {
        groupId: 'cascade-aggregator',
      },
    },
  });

  await app.startAllMicroservices();
  await app.listen(config.PORT);
  Logger.log(
    `Aggregator: health on http://localhost:${config.PORT}, ` +
      `consuming from ${config.KAFKA_BOOTSTRAP_SERVERS.join(', ')}`,
    'Bootstrap',
  );
}

void bootstrap();
