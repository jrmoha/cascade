import { Type } from 'class-transformer';
import { IsInt, IsNotEmpty, IsOptional, IsString, Max, Min } from 'class-validator';

/**
 * Query string for `GET /query`. `projectId` selects the tenant; `hours` is the
 * hourly-bucket lookback (default 1 = current hour only). It is capped to bound
 * the number of single-partition reads we fan out — see RawEventReadRepository.
 */
export class QueryEventsDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(168)
  hours: number = 1;
}
