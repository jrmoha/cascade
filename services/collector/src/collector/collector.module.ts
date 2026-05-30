import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { CollectController } from './collect.controller';
import { CollectorService } from './collector.service';
import { KAFKA_PRODUCER } from './kafka.tokens';

@Module({
  imports: [
    ClientsModule.registerAsync([
      {
        name: KAFKA_PRODUCER,
        inject: [ConfigService],
        useFactory: (config: ConfigService) => {
          const brokers = (config.get<string>('KAFKA_BOOTSTRAP_SERVERS') ?? 'localhost:9092')
            .split(',')
            .map((b) => b.trim())
            .filter(Boolean);

          return {
            transport: Transport.KAFKA,
            options: {
              client: {
                clientId: 'cascade-collector',
                brokers,
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
