import { Module } from '@nestjs/common';
import { PostgresModule } from '../postgres/postgres.module';
import { RetentionController } from './retention.controller';
import { RetentionService } from './retention.service';

@Module({
  imports: [PostgresModule],
  controllers: [RetentionController],
  providers: [RetentionService],
})
export class RetentionModule {}
