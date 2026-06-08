import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService } from '@nestjs/terminus';
import { CassandraHealthIndicator } from './cassandra.health';
import { RedisHealthIndicator } from '../redis/redis.health';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = the read stores the Query API serves from are
 * reachable: Cassandra (bounded raw retrieval, ADR-0008) and Redis (the
 * leaderboard read model, KAN-34). Either down → 503. Consumed by
 * container/k8s probes (KAN-27).
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly cassandra: CassandraHealthIndicator,
    private readonly redis: RedisHealthIndicator,
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
      () => this.cassandra.isHealthy('cassandra'),
      () => this.redis.isHealthy('redis'),
    ]);
  }
}
