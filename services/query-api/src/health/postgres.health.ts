import { Injectable } from '@nestjs/common';
import { HealthIndicatorService, type HealthIndicatorResult } from '@nestjs/terminus';
import { PostgresService } from '../postgres/postgres.service';

/**
 * Readiness indicator for Postgres: a `SELECT 1`. Postgres holds the funnel and
 * retention summary tables the Query API serves (KAN-35), so if it is
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
