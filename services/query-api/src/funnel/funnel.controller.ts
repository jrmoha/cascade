import { BadRequestException, Controller, Get, Query } from '@nestjs/common';
import { type FunnelResponse, funnelStepsSchema } from '@cascade/contracts';
import { FunnelQueryDto } from './dto/funnel-query.dto';
import { FunnelService } from './funnel.service';

@Controller('funnel')
export class FunnelController {
  constructor(private readonly funnel: FunnelService) {}

  /**
   * Ordered funnel conversion for a `(projectId, steps, [from, to])` (KAN-35).
   * Served from the Aggregator's per-actor step summary in Postgres — never by
   * scanning raw events (ADR-0015 / ADR-0017).
   */
  @Get()
  async query(@Query() dto: FunnelQueryDto): Promise<FunnelResponse> {
    const { projectId, from, to } = dto;

    // Parse `?steps=a,b,c` and validate (2–10 distinct event types) here at the
    // HTTP edge so we can return a 400 with context.
    const rawSteps = dto.steps
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    const parsed = funnelStepsSchema.safeParse(rawSteps);
    if (!parsed.success) {
      throw new BadRequestException(parsed.error.issues[0]?.message ?? 'invalid funnel steps');
    }

    if (new Date(from).getTime() > new Date(to).getTime()) {
      throw new BadRequestException('`from` must be the same as or before `to`');
    }

    return this.funnel.compute({ projectId, steps: parsed.data, from, to });
  }
}
