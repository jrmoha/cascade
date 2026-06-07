import { Injectable } from '@nestjs/common';
import { HealthIndicatorService, type HealthIndicatorResult } from '@nestjs/terminus';
import { CassandraService } from '../cassandra/cassandra.service';

/**
 * Readiness indicator for Cassandra: issues a trivial, single-node query
 * (`SELECT now() FROM system.local`) through the existing client. It verifies
 * the driver has a live connection to the cluster without touching application
 * data. Reports `down` (→ 503) when the cluster is unreachable.
 */
@Injectable()
export class CassandraHealthIndicator {
  constructor(
    private readonly cassandra: CassandraService,
    private readonly healthIndicator: HealthIndicatorService,
  ) {}

  async isHealthy(key: string): Promise<HealthIndicatorResult> {
    const indicator = this.healthIndicator.check(key);
    try {
      await this.cassandra.execute('SELECT now() FROM system.local');
      return indicator.up();
    } catch (err) {
      return indicator.down({ message: (err as Error).message });
    }
  }
}
