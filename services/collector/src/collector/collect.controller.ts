import { Body, Controller, HttpCode, HttpStatus, Post, UseGuards } from '@nestjs/common';
import { CollectEventInput, collectEventSchema } from '@cascade/contracts';
import { CollectorService } from './collector.service';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { ApiKeyGuard } from '../ingest/api-key.guard';
import { RateLimitGuard } from '../ingest/rate-limit.guard';
import { ProjectId } from '../ingest/project-id.decorator';

@Controller()
export class CollectController {
  constructor(private readonly collectorService: CollectorService) {}

  /**
   * Accept an event and produce it to Kafka. Checks run cheapest-decisive first
   * (KAN-30, KAN-42):
   *  0. {@link RateLimitGuard} caps per-API-key throughput before auth — over
   *     budget is a `429` with `Retry-After` (ADR-0021).
   *  1. {@link ApiKeyGuard} authenticates the `x-api-key` header and resolves
   *     the `projectId` — a missing/invalid/revoked key is a `401`.
   *  2. the body is validated against `collectEventSchema` (the shared envelope
   *     contract) — a bad envelope is a structured `400`.
   *  3. the service validates the payload against the project's registered JSON
   *     Schema — an unregistered type is `422`, a bad payload `400`.
   *
   * `projectId` comes from the key, never the body. Valid events are
   * fire-and-acknowledged: `202` once handed to the broker. Under backpressure
   * or after exhausted produce retries the service returns `503` (the event was
   * never acknowledged, so the client can safely retry — nothing is dropped).
   */
  @Post('collect')
  @UseGuards(RateLimitGuard, ApiKeyGuard)
  @HttpCode(HttpStatus.ACCEPTED)
  async collect(
    @ProjectId() projectId: string,
    @Body(new ZodValidationPipe(collectEventSchema)) input: CollectEventInput,
  ): Promise<{ eventId: string; status: 'accepted' }> {
    const eventId = await this.collectorService.collect(projectId, input);
    return { eventId, status: 'accepted' };
  }
}
