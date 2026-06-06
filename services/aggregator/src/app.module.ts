import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CassandraModule } from './cassandra/cassandra.module';
import { RedisModule } from './redis/redis.module';
import { PostgresModule } from './postgres/postgres.module';
import { AggregationModule } from './aggregation/aggregation.module';
import { HealthModule } from './health/health.module';

@Module({
  imports: [
    AppConfigModule,
    CassandraModule,
    RedisModule,
    PostgresModule,
    AggregationModule,
    HealthModule,
  ],
})
export class AppModule {}
