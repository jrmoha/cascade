import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { CollectorModule } from './collector/collector.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), CollectorModule],
})
export class AppModule {}
