import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { CassandraModule } from '../cassandra/cassandra.module';
import { CassandraHealthIndicator } from './cassandra.health';
import { HealthController } from './health.controller';

@Module({
  imports: [TerminusModule, CassandraModule],
  controllers: [HealthController],
  providers: [CassandraHealthIndicator],
})
export class HealthModule {}
