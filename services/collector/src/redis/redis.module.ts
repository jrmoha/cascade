import { Global, Inject, Logger, Module, type OnApplicationShutdown } from '@nestjs/common';
import Redis from 'ioredis';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';
import { REDIS_CLIENT } from './redis.tokens';

/**
 * Provides a single, shared {@link Redis} client (ioredis) built from the
 * validated {@link CollectorConfig}. Global so the ingest cache and the Redis
 * health indicator inject the same connection.
 *
 * `lazyConnect` keeps construction side-effect-free (the socket opens on first
 * command), which matters for tests that build the module without a live Redis.
 * `maxRetriesPerRequest: 1` bounds how long a command waits when Redis is down,
 * so a cache lookup fails fast into the fail-closed path rather than hanging the
 * ingest request (ADR-0013).
 */
@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [APP_CONFIG],
      useFactory: (config: CollectorConfig): Redis => {
        const client = new Redis({
          host: config.REDIS_HOST,
          port: config.REDIS_PORT,
          lazyConnect: true,
          maxRetriesPerRequest: 1,
        });
        client.on('error', (err) => {
          // Connection-level errors are logged but never thrown here — callers
          // handle command failures inline (fail-closed on a cold cache).
          new Logger('RedisModule').warn(`Redis connection error: ${err.message}`);
        });
        return client;
      },
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS_CLIENT) private readonly client: Redis) {}

  /** Close the connection cleanly on shutdown so the process can exit. */
  async onApplicationShutdown(): Promise<void> {
    await this.client.quit().catch(() => undefined);
  }
}
