import { Controller, Get, Inject } from '@nestjs/common';
import { Transport } from '@nestjs/microservices';
import { HealthCheck, HealthCheckService, MicroserviceHealthIndicator } from '@nestjs/terminus';
import { APP_CONFIG } from '../config/config.module';
import type { AggregatorConfig } from '../config/env.schema';
import { CassandraHealthIndicator } from '../cassandra/cassandra.health';
import { RedisHealthIndicator } from '../redis/redis.health';
import { PostgresHealthIndicator } from '../postgres/postgres.health';

/**
 * Liveness (`GET /health`) = the process is up and serving HTTP.
 * Readiness (`GET /ready`) = every downstream dependency is reachable: Kafka
 * (the topic it consumes / dead-letters to) and the three read-model stores it
 * writes — Cassandra (counters), Redis (leaderboards/dedup), Postgres
 * (funnel/retention) — see ADR-0015. Any one down → 503 and the service is
 * pulled from rotation.
 */
@Controller()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly microservice: MicroserviceHealthIndicator,
    private readonly cassandra: CassandraHealthIndicator,
    private readonly redis: RedisHealthIndicator,
    private readonly postgres: PostgresHealthIndicator,
    @Inject(APP_CONFIG) private readonly config: AggregatorConfig,
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
      () => this.redis.isHealthy('redis'),
      () => this.postgres.isHealthy('postgres'),
    ]);
  }
}
