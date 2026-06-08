import { Global, Inject, Logger, Module, type OnApplicationShutdown } from '@nestjs/common';
import Redis from 'ioredis';
import { APP_CONFIG } from '../config/config.module';
import type { QueryApiConfig } from '../config/env.schema';
import { REDIS_CLIENT } from './redis.tokens';

/**
 * Provides a single, shared {@link Redis} client (ioredis) built from the
 * validated {@link QueryApiConfig}. Global so the leaderboard reader and the
 * Redis health indicator inject the same connection. Mirrors the Aggregator's
 * `RedisModule` — the Query API now serves the leaderboard read model from the
 * same Redis the Aggregator writes (ADR-0015, KAN-34).
 *
 * `lazyConnect` keeps construction side-effect-free (the socket opens on first
 * command), which matters for tests that build the module without a live Redis.
 */
@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [APP_CONFIG],
      useFactory: (config: QueryApiConfig): Redis => {
        const client = new Redis({
          host: config.REDIS_HOST,
          port: config.REDIS_PORT,
          lazyConnect: true,
          maxRetriesPerRequest: 1,
        });
        client.on('error', (err) => {
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
