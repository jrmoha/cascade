import { Module } from '@nestjs/common';
import { LeaderboardController } from './leaderboard.controller';
import { LeaderboardService } from './leaderboard.service';

/**
 * Leaderboard read endpoints (KAN-34). `REDIS_CLIENT` is injected from the global
 * `RedisModule`, so no imports are needed here.
 */
@Module({
  controllers: [LeaderboardController],
  providers: [LeaderboardService],
})
export class LeaderboardModule {}
