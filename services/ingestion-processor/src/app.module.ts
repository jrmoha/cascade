import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CassandraModule } from './cassandra/cassandra.module';
import { ProcessorModule } from './processor/processor.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), CassandraModule, ProcessorModule],
})
export class AppModule {}
