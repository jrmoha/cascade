import { Module } from '@nestjs/common';
import { CassandraModule } from '../cassandra/cassandra.module';
import { ProcessorController } from './processor.controller';
import { RawEventRepository } from './raw-event.repository';

@Module({
  imports: [CassandraModule],
  controllers: [ProcessorController],
  providers: [RawEventRepository],
})
export class ProcessorModule {}
