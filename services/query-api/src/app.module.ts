import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CassandraModule } from './cassandra/cassandra.module';
import { QueryModule } from './query/query.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), CassandraModule, QueryModule],
})
export class AppModule {}
