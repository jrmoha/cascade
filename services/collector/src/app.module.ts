import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CollectorModule } from './collector/collector.module';
import { HealthModule } from './health/health.module';

@Module({
  imports: [AppConfigModule, CollectorModule, HealthModule],
})
export class AppModule {}
