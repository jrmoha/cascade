import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { CassandraModule } from '../cassandra/cassandra.module';
import { PostgresModule } from '../postgres/postgres.module';
import { CassandraHealthIndicator } from './cassandra.health';
import { RedisHealthIndicator } from '../redis/redis.health';
import { PostgresHealthIndicator } from './postgres.health';
import { HealthController } from './health.controller';

@Module({
  imports: [TerminusModule, CassandraModule, PostgresModule],
  controllers: [HealthController],
  providers: [CassandraHealthIndicator, RedisHealthIndicator, PostgresHealthIndicator],
})
export class HealthModule {}
