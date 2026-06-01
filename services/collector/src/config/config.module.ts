import { join } from 'node:path';
import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { collectorEnvSchema, type CollectorConfig } from './env.schema';

/** DI token for the validated, fully-typed service configuration. */
export const APP_CONFIG = 'APP_CONFIG';

/**
 * Loads the `.env` file (local dev; absent in containers where compose injects
 * env) and exposes a frozen, fully-typed {@link CollectorConfig} as a global
 * provider. The Zod parse is the single validation point and runs once at boot:
 * a missing/invalid var throws and the process exits before serving traffic
 * (fail-fast, 12-factor). Injecting `ConfigService` into the factory guarantees
 * the env file is loaded into `process.env` before we parse it.
 */
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
      useFactory: (): CollectorConfig => Object.freeze(collectorEnvSchema.parse(process.env)),
    },
  ],
  exports: [APP_CONFIG],
})
export class AppConfigModule {}
