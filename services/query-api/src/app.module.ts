import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CassandraModule } from './cassandra/cassandra.module';
import { HealthModule } from './health/health.module';
import { QueryModule } from './query/query.module';

@Module({
  imports: [AppConfigModule, CassandraModule, QueryModule, HealthModule],
})
export class AppModule {}
