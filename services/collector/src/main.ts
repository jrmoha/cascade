import 'reflect-metadata';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { APP_CONFIG } from './config/config.module';
import type { CollectorConfig } from './config/env.schema';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  const config = app.get<CollectorConfig>(APP_CONFIG);
  await app.listen(config.PORT);
  Logger.log(`Collector listening on http://localhost:${config.PORT}`, 'Bootstrap');
}

void bootstrap();
