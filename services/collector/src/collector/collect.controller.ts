import { Body, Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { CollectEventDto } from './dto/collect-event.dto';
import { CollectorService } from './collector.service';

@Controller()
export class CollectController {
  constructor(private readonly collectorService: CollectorService) {}

  /**
   * Accept an event and produce it to Kafka. Fire-and-acknowledge: returns
   * 202 once the message has been handed to the broker.
   */
  @Post('collect')
  @HttpCode(HttpStatus.ACCEPTED)
  async collect(@Body() dto: CollectEventDto): Promise<{ eventId: string; status: 'accepted' }> {
    const eventId = await this.collectorService.collect(dto);
    return { eventId, status: 'accepted' };
  }
}
