import { Injectable } from '@nestjs/common';
import { HealthIndicatorService, type HealthIndicatorResult } from '@nestjs/terminus';
import { DatabaseService } from '../db/database.service';

/**
 * Readiness indicator for Postgres: a trivial `SELECT 1` over the Prisma client.
 * If the database is unreachable the check reports `down`, so `GET /ready`
 * returns 503 and the service is pulled from rotation.
 */
@Injectable()
export class PrismaHealthIndicator {
  constructor(
    private readonly db: DatabaseService,
    private readonly healthIndicator: HealthIndicatorService,
  ) {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const indicator = this.healthIndicator.check(key);
    try {
      await this.db.$queryRaw`SELECT 1`;
      return indicator.up();
    } catch (err) {
      return indicator.down({ message: (err as Error).message });
    }
  }
}
