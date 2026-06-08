import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CassandraModule } from './cassandra/cassandra.module';
import { RedisModule } from './redis/redis.module';
import { HealthModule } from './health/health.module';
import { LeaderboardModule } from './leaderboard/leaderboard.module';
import { QueryModule } from './query/query.module';

@Module({
  imports: [
    AppConfigModule,
    CassandraModule,
    RedisModule,
    QueryModule,
    LeaderboardModule,
    HealthModule,
  ],
})
export class AppModule {}
