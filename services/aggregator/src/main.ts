import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
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
      // ServerKafka spins up an internal producer (used for request-reply); pin
      // its partitioner so it doesn't emit KafkaJS's "default partitioner
      // switched" warning and stays consistent with the rest of the system
      // (Collector + DLQ producers all use DefaultPartitioner — ADR-0002).
      producer: {
        createPartitioner: Partitioners.DefaultPartitioner,
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
