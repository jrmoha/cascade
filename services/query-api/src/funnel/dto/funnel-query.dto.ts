import { IsISO8601, IsNotEmpty, IsString } from 'class-validator';

/**
 * Query string for `GET /funnel` (KAN-35). Computes, for an **ordered** sequence
 * of event types, how many distinct actors progressed through each step within an
 * inclusive event-time window `[from, to]`.
 *
 * `steps` is a comma-separated list of event types (e.g.
 * `?steps=game_start,level_complete,purchase`). It is parsed and validated
 * (2–10 distinct types) in the controller via the shared `funnelStepsSchema`,
 * which has the request context to return a meaningful `400`. `from`/`to` are
 * ISO-8601 instants bounding `occurredAt`.
 */
export class FunnelQueryDto {
  @IsString()
  @IsNotEmpty()
  projectId!: string;

  @IsString()
  @IsNotEmpty()
  steps!: string;

  @IsISO8601()
  from!: string;

  @IsISO8601()
  to!: string;
}
