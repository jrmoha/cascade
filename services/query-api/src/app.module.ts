import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { CassandraModule } from './cassandra/cassandra.module';
import { RedisModule } from './redis/redis.module';
import { PostgresModule } from './postgres/postgres.module';
import { HealthModule } from './health/health.module';
import { LeaderboardModule } from './leaderboard/leaderboard.module';
import { FunnelModule } from './funnel/funnel.module';
import { RetentionModule } from './retention/retention.module';
import { QueryModule } from './query/query.module';

@Module({
  imports: [
    AppConfigModule,
    CassandraModule,
    RedisModule,
    PostgresModule,
    QueryModule,
    LeaderboardModule,
    FunnelModule,
    RetentionModule,
    HealthModule,
  ],
})
export class AppModule {}
