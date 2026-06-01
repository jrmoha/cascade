import { join } from 'node:path';
import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ingestionEnvSchema, type IngestionConfig } from './env.schema';

/** DI token for the validated, fully-typed service configuration. */
export const APP_CONFIG = 'APP_CONFIG';

/**
 * Loads the `.env` file (local dev; absent in containers where compose injects
 * env) and exposes a frozen, fully-typed {@link IngestionConfig} as a global
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
      useFactory: (): IngestionConfig => Object.freeze(ingestionEnvSchema.parse(process.env)),
    },
  ],
  exports: [APP_CONFIG],
})
export class AppConfigModule {}
