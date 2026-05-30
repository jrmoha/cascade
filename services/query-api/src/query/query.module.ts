import { Module } from '@nestjs/common';
import { CassandraModule } from '../cassandra/cassandra.module';
import { QueryController } from './query.controller';
import { RawEventReadRepository } from './raw-event.read-repository';

@Module({
  imports: [CassandraModule],
  controllers: [QueryController],
  providers: [RawEventReadRepository],
})
export class QueryModule {}
