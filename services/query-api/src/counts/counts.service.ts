import { Injectable } from '@nestjs/common';
import type { CountsGranularity, CountsResponse } from '@cascade/contracts';
import { CountsRepository } from './counts.repository';

export interface CountsQuery {
  projectId: string;
  from: string;
  to: string;
  granularity: CountsGranularity;
  type?: string;
}

/**
 * Serves the `GET /counts` time-series from the Aggregator's event-count read
 * models (KAN-36). Thin orchestration over {@link CountsRepository}: the read
 * model already holds the counts, so there is no aggregation at read time —
 * exactly the CQRS payoff (ADR-0015 / ADR-0018).
 */
@Injectable()
export class CountsService {
  constructor(private readonly repository: CountsRepository) {}

  async compute(query: CountsQuery): Promise<CountsResponse> {
    const { projectId, from, to, granularity, type } = query;
    const buckets = await this.repository.read({ projectId, from, to, granularity, type });
    return { projectId, granularity, from, to, buckets };
  }
}
