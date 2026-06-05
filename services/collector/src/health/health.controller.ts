import { Controller, Get, Inject } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { HealthCheck, HealthCheckService, MicroserviceHealthIndicator } from '@nestjs/terminus';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';
import { RedisHealthIndicator } from '../redis/redis.health';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = the Collector's downstream dependencies are
 * reachable — **Kafka** (events are produced there) and **Redis** (the ingest
 * key/schema cache, KAN-30). If either is down the Collector cannot accept
 * events and should be pulled from rotation. Consumed by container/k8s probes
 * (KAN-27). Project/Schema is *not* a readiness dep — the cache + fail-closed
 * path (ADR-0013) handle its absence per-request.
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly microservice: MicroserviceHealthIndicator,
    private readonly redis: RedisHealthIndicator,
    @Inject(APP_CONFIG) private readonly config: CollectorConfig,
  ) {}

  @Get('health')
  @HealthCheck()
  live() {
    return this.health.check([]);
  }

  @Get('ready')
  @HealthCheck()
  ready() {
    return this.health.check([
      () =>
        this.microservice.pingCheck('kafka', {
          transport: Transport.KAFKA,
          timeout: 5000,
          options: {
            client: { brokers: this.config.KAFKA_BOOTSTRAP_SERVERS },
          },
        }),
      () => this.redis.isHealthy('redis'),
    ]);
  }
}
