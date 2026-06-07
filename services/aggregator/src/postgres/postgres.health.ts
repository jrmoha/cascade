import { Injectable } from '@nestjs/common';
import { HealthIndicatorService, type HealthIndicatorResult } from '@nestjs/terminus';
import { PostgresService } from './postgres.service';

/**
 * Readiness indicator for Postgres: a trivial `SELECT 1` through the pool. The
 * Aggregator's funnel/retention summaries live here (ADR-0015), so if it is
 * unreachable `GET /ready` reports `down` (→ 503) and the service is pulled from
 * rotation.
 */
@Injectable()
export class PostgresHealthIndicator {
  constructor(
    private readonly postgres: PostgresService,
    private readonly healthIndicator: HealthIndicatorService,
  ) {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const indicator = this.healthIndicator.check(key);
    try {
      await this.postgres.query('SELECT 1');
      return indicator.up();
    } catch (err) {
      return indicator.down({ message: (err as Error).message });
    }
  }
}
