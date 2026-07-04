import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { RETENTION_MAX_COHORT_DAYS, type RetentionResponse } from '@cascade/contracts';
import { RetentionQueryDto } from './dto/retention-query.dto';
import { RetentionService } from './retention.service';

const MS_PER_DAY = 86_400_000;

@Controller('retention')
export class RetentionController {
  constructor(private readonly retention: RetentionService) {}

  /**
   * Retention cohort matrix for a `(projectId, [from, to] cohort range)` (KAN-35).
   * Served from the Aggregator's per-actor active-day summary in Postgres — never
   * by scanning raw events (ADR-0015 / ADR-0017).
   */
  @Get()
  async query(@Query() dto: RetentionQueryDto): Promise<RetentionResponse> {
    const { projectId, from, to, maxOffset } = dto;

    const spanDays = Math.round(
      (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / MS_PER_DAY,
    );
    if (Number.isNaN(spanDays) || spanDays < 0) {
      throw new BadRequestException('`from` must be the same as or before `to`');
    }
    if (spanDays + 1 > RETENTION_MAX_COHORT_DAYS) {
      throw new BadRequestException(
        `Cohort range spans ${spanDays + 1} days, exceeding the maximum of ${RETENTION_MAX_COHORT_DAYS}`,
      );
    }

    return this.retention.compute({ projectId, from, to, maxOffset });
  }
}
