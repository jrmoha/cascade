import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, type HealthIndicatorResult } from '@nestjs/terminus';
import { PostgresService } from './postgres.service';

/**
 * Readiness indicator for Postgres: a trivial `SELECT 1` through the pool. The
 * Aggregator's funnel/retention summaries live here (ADR-0015), so if it is
 * unreachable `GET /ready` returns 503 and the service is pulled from rotation.
 */
@Injectable()
export class PostgresHealthIndicator extends HealthIndicator {
  constructor(private readonly postgres: PostgresService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      await this.postgres.query('SELECT 1');
      return this.getStatus(key, true);
    } catch (err) {
      throw new HealthCheckError(
        'Postgres check failed',
        this.getStatus(key, false, { message: (err as Error).message }),
      );
    }
  }
}
