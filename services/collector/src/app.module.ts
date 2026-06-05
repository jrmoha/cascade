import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CollectorModule } from './collector/collector.module';
import { HealthModule } from './health/health.module';
import { RedisModule } from './redis/redis.module';

@Module({
  imports: [AppConfigModule, RedisModule, CollectorModule, HealthModule],
})
export class AppModule {}
