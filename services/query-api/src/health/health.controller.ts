import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { CassandraHealthIndicator } from './cassandra.health';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = Cassandra (the read store the Query API serves
 * from) is reachable. Both are consumed by container/k8s probes (KAN-27).
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly cassandra: CassandraHealthIndicator,
  ) {}

  @Get('health')
  @HealthCheck()
  live() {
    return this.health.check([]);
  }

  @Get('ready')
  @HealthCheck()
  ready() {
    return this.health.check([() => this.cassandra.isHealthy('cassandra')]);
  }
}
