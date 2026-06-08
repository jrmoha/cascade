import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { CassandraModule } from '../cassandra/cassandra.module';
import { CassandraHealthIndicator } from './cassandra.health';
import { RedisHealthIndicator } from '../redis/redis.health';
import { HealthController } from './health.controller';

@Module({
  imports: [TerminusModule, CassandraModule],
  controllers: [HealthController],
  providers: [CassandraHealthIndicator, RedisHealthIndicator],
})
export class HealthModule {}
