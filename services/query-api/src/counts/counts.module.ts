import { Module } from '@nestjs/common';
import { CassandraModule } from '../cassandra/cassandra.module';
import { CountsController } from './counts.controller';
import { CountsRepository } from './counts.repository';
import { CountsService } from './counts.service';

@Module({
  imports: [CassandraModule],
  controllers: [CountsController],
  providers: [CountsService, CountsRepository],
})
export class CountsModule {}
