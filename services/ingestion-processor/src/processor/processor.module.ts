import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { CassandraModule } from '../cassandra/cassandra.module';
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
        inject: [ConfigService],
        useFactory: (config: ConfigService) => {
          const brokers = (config.get<string>('KAFKA_BOOTSTRAP_SERVERS') ?? 'localhost:9092')
            .split(',')
            .map((b) => b.trim())
            .filter(Boolean);

          return {
            transport: Transport.KAFKA,
            options: {
              client: { clientId: 'cascade-ingestion-processor-dlq', brokers },
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
