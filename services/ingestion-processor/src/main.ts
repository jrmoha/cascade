import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { AppModule } from './app.module';
import { APP_CONFIG } from './config/config.module';
import type { IngestionConfig } from './config/env.schema';

/**
 * Hybrid app: a thin HTTP server (so the process can expose `/health` and
 * `/ready` probes for containers/k8s — KAN-27) with the Kafka consumer attached
 * as a connected microservice. The `@EventPattern(RAW_EVENTS_TOPIC)` handler and
 * the `cascade-ingestion-processor` consumer group are unchanged; NestJS still
 * postfixes the broker-side group with `-server`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const config = app.get<IngestionConfig>(APP_CONFIG);

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'cascade-ingestion-processor',
        brokers: config.KAFKA_BOOTSTRAP_SERVERS,
      },
      consumer: {
        groupId: 'cascade-ingestion-processor',
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
    `Ingestion-Processor: health on http://localhost:${config.PORT}, ` +
      `consuming from ${config.KAFKA_BOOTSTRAP_SERVERS.join(', ')}`,
    'Bootstrap',
  );
}

void bootstrap();
