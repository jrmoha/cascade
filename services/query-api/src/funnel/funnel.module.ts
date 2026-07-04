import { Module } from '@nestjs/common';
import { PostgresModule } from '../postgres/postgres.module';
import { FunnelController } from './funnel.controller';
import { FunnelService } from './funnel.service';

@Module({
  imports: [PostgresModule],
  controllers: [FunnelController],
  providers: [FunnelService],
})
export class FunnelModule {}
