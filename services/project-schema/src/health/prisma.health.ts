import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, type HealthIndicatorResult } from '@nestjs/terminus';
import { DatabaseService } from '../db/database.service';

/**
 * Readiness indicator for Postgres: a trivial `SELECT 1` over the Prisma client.
 * If the database is unreachable the check throws, so `GET /ready` returns 503
 * and the service is pulled from rotation.
 */
@Injectable()
export class PrismaHealthIndicator extends HealthIndicator {
  constructor(private readonly db: DatabaseService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      await this.db.$queryRaw`SELECT 1`;
      return this.getStatus(key, true);
    } catch (err) {
      throw new HealthCheckError(
        'Postgres check failed',
        this.getStatus(key, false, { message: (err as Error).message }),
      );
    }
  }
}
