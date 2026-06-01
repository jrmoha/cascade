import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { CassandraModule } from '../cassandra/cassandra.module';
import { APP_CONFIG } from '../config/config.module';
import type { IngestionConfig } from '../config/env.schema';
import { ProcessorController } from './processor.controller';
import { RawEventRepository } from './raw-event.repository';
import { DeadLetterPublisher } from './dead-letter.publisher';
import { DLQ_PRODUCER } from './kafka.tokens';

@Module({
  imports: [
    CassandraModule,
    ClientsModule.registerAsync([
      {
        name: DLQ_PRODUCER,
        inject: [APP_CONFIG],
        useFactory: (config: IngestionConfig) => {
          return {
            transport: Transport.KAFKA,
            options: {
              client: {
                clientId: 'cascade-ingestion-processor-dlq',
                brokers: config.KAFKA_BOOTSTRAP_SERVERS,
              },
              // Match the Collector's partitioner so keys hash consistently (ADR-0002).
              producer: { createPartitioner: Partitioners.DefaultPartitioner },
              producerOnlyMode: true,
            },
          };
        },
      },
    ]),
  ],
  controllers: [ProcessorController],
  providers: [RawEventRepository, DeadLetterPublisher],
})
export class ProcessorModule {}
