import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const brokers = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'cascade-ingestion-processor',
        brokers,
      },
      consumer: {
        groupId: 'cascade-ingestion-processor',
      },
    },
  });

  await app.listen();
  Logger.log(`Ingestion-Processor consuming from brokers: ${brokers.join(', ')}`, 'Bootstrap');
}

void bootstrap();
