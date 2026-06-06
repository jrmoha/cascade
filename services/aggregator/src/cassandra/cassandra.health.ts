import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicator, type HealthIndicatorResult } from '@nestjs/terminus';
import { CassandraService } from './cassandra.service';

/**
 * Readiness indicator for Cassandra: a trivial single-node query
 * (`SELECT now() FROM system.local`) through the existing client to confirm the
 * driver has a live connection. Throws (→ 503) when the cluster is unreachable.
 */
@Injectable()
export class CassandraHealthIndicator extends HealthIndicator {
  constructor(private readonly cassandra: CassandraService) {
    super();
  }

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    try {
      await this.cassandra.execute('SELECT now() FROM system.local');
      return this.getStatus(key, true);
    } catch (err) {
      throw new HealthCheckError(
        'Cassandra check failed',
        this.getStatus(key, false, { message: (err as Error).message }),
      );
    }
  }
}
