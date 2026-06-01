import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CassandraModule } from './cassandra/cassandra.module';
import { HealthModule } from './health/health.module';
import { ProcessorModule } from './processor/processor.module';

@Module({
  imports: [AppConfigModule, CassandraModule, ProcessorModule, HealthModule],
})
export class AppModule {}
