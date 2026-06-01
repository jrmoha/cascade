import { Controller, Get, Inject } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { HealthCheck, HealthCheckService, MicroserviceHealthIndicator } from '@nestjs/terminus';
import { APP_CONFIG } from '../config/config.module';
import type { IngestionConfig } from '../config/env.schema';
import { CassandraHealthIndicator } from './cassandra.health';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = both downstream dependencies are reachable: Kafka
 * (the topic it consumes / dead-letters to) and Cassandra (the write store).
 * Served over the hybrid app's HTTP port; consumed by container/k8s probes.
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly microservice: MicroserviceHealthIndicator,
    private readonly cassandra: CassandraHealthIndicator,
    @Inject(APP_CONFIG) private readonly config: IngestionConfig,
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
      () => this.cassandra.isHealthy('cassandra'),
    ]);
  }
}
