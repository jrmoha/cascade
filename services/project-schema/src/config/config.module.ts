import { join } from 'node:path';
import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { projectSchemaEnvSchema, type ProjectSchemaConfig } from './env.schema';

export const APP_CONFIG = 'APP_CONFIG';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: [join(process.cwd(), '.env'), join(__dirname, '..', '..', '..', '..', '.env')],
    }),
  ],
  providers: [
    {
      provide: APP_CONFIG,
      inject: [ConfigService],
      useFactory: (): ProjectSchemaConfig =>
        Object.freeze(projectSchemaEnvSchema.parse(process.env)),
    },
  ],
  exports: [APP_CONFIG],
})
export class AppConfigModule {}
