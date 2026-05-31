import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { CollectEventInput, collectEventSchema } from '@cascade/contracts';
import { CollectorService } from './collector.service';
import { ZodValidationPipe } from '../common/zod-validation.pipe';

@Controller()
export class CollectController {
  constructor(private readonly collectorService: CollectorService) {}

  /**
   * Accept an event and produce it to Kafka. The body is validated at the edge
   * against `collectEventSchema` (derived from the shared contract); invalid
   * events are rejected with a structured 400 and never reach Kafka. Valid
   * events are fire-and-acknowledged: returns 202 once handed to the broker.
   */
  @Post('collect')
  @HttpCode(HttpStatus.ACCEPTED)
  async collect(
    @Body(new ZodValidationPipe(collectEventSchema)) input: CollectEventInput,
  ): Promise<{ eventId: string; status: 'accepted' }> {
    const eventId = await this.collectorService.collect(input);
    return { eventId, status: 'accepted' };
  }
}
