import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { Partitioners } from 'kafkajs';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { CassandraModule } from '../cassandra/cassandra.module';
import { PostgresModule } from '../postgres/postgres.module';
import { AggregatorController } from './aggregator.controller';
import { DeadLetterPublisher } from './dead-letter.publisher';
import { DedupStore } from './dedup.store';
import { EventCountsRepository } from './event-counts.repository';
import { LeaderboardRepository } from './leaderboard.repository';
import { FunnelRepository } from './funnel.repository';
import { RetentionRepository } from './retention.repository';
import { DLQ_PRODUCER } from './kafka.tokens';

@Module({
  imports: [
    // CassandraService for the counter writer; PostgresService for the
    // funnel/retention writers; RedisModule is @Global so the dedup store's
    // REDIS_CLIENT is already injectable.
    CassandraModule,
    PostgresModule,
    ClientsModule.registerAsync([
      {
        name: DLQ_PRODUCER,
        inject: [APP_CONFIG],
        useFactory: (config: AggregatorConfig) => {
          return {
            transport: Transport.KAFKA,
            options: {
              client: {
                clientId: 'cascade-aggregator-dlq',
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
  controllers: [AggregatorController],
  providers: [
    DeadLetterPublisher,
    DedupStore,
    EventCountsRepository,
    LeaderboardRepository,
    FunnelRepository,
    RetentionRepository,
  ],
})
export class AggregationModule {}
