import { Controller, Get, Query } from '@nestjs/common';
import { RawEvent } from '@cascade/contracts';
import { QueryEventsDto } from './dto/query-events.dto';
import { RawEventReadRepository } from './raw-event.read-repository';

@Controller()
export class QueryController {
  constructor(private readonly repository: RawEventReadRepository) {}

  /**
   * Read stored events back for a project. Phase 0 walking-skeleton endpoint:
   * returns raw events straight from Cassandra (no aggregation) to prove the
   * ingest→store→read loop. See ADR-0003 for why this is temporary.
   */
  @Get('query')
  async query(
    @Query() dto: QueryEventsDto,
  ): Promise<{ projectId: string; hours: number; count: number; events: RawEvent[] }> {
    const events = await this.repository.readRecent(dto.projectId, dto.hours);
    return { projectId: dto.projectId, hours: dto.hours, count: events.length, events };
  }
}
