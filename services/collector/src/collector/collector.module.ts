import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';
import { IngestModule } from '../ingest/ingest.module';
import { CollectController } from './collect.controller';
import { CollectorService } from './collector.service';
import { KAFKA_PRODUCER } from './kafka.tokens';

@Module({
  imports: [
    IngestModule,
    ClientsModule.registerAsync([
      {
        name: KAFKA_PRODUCER,
        inject: [APP_CONFIG],
        useFactory: (config: CollectorConfig) => {
          return {
            transport: Transport.KAFKA,
            options: {
              client: {
                clientId: 'cascade-collector',
                brokers: config.KAFKA_BOOTSTRAP_SERVERS,
              },
              // Pin the partitioner explicitly. DefaultPartitioner uses a
              // Java-client-compatible murmur2 hash of the message key, so our
              // projectId keying stays consistent with other Kafka tooling.
              // Also silences KafkaJS's v2 "default partitioner changed" warning.
              producer: {
                createPartitioner: Partitioners.DefaultPartitioner,
              },
              producerOnlyMode: true,
            },
          };
        },
      },
    ]),
  ],
  controllers: [CollectController],
  providers: [CollectorService],
})
export class CollectorModule {}
