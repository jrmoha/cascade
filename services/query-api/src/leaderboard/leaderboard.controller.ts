import { Controller, Get, Query } from '@nestjs/common';
import type { PlayerRankResponse, TopNResponse } from '@cascade/contracts';
import { LeaderboardService } from './leaderboard.service';
import { PlayerRankQueryDto } from './dto/player-rank-query.dto';
import { TopNQueryDto } from './dto/top-n-query.dto';

@Controller('leaderboard')
export class LeaderboardController {
  constructor(private readonly leaderboard: LeaderboardService) {}

  @Get()
  topN(@Query() dto: TopNQueryDto): Promise<TopNResponse> {
    return this.leaderboard.topN(dto);
  }

  @Get('rank')
  playerRank(@Query() dto: PlayerRankQueryDto): Promise<PlayerRankResponse> {
    return this.leaderboard.playerRank(dto);
  }
}
