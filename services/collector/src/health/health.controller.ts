import { Controller, Get, Inject } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { HealthCheck, HealthCheckService, MicroserviceHealthIndicator } from '@nestjs/terminus';
import { APP_CONFIG } from '../config/config.module';
import type { CollectorConfig } from '../config/env.schema';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = the Collector's only downstream dependency, Kafka,
 * is reachable — if it isn't, the Collector cannot accept events and should be
 * pulled from rotation. Both are consumed by container/k8s probes (KAN-27).
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly microservice: MicroserviceHealthIndicator,
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
    ]);
  }
}
