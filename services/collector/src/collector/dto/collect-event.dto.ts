import { IsISO8601, IsObject, IsOptional, IsString, MaxLength, MinLength } from 'class-validator';

/**
 * HTTP input for `POST /collect`. This is a deliberate **subset** of the
 * canonical `rawEventSchema` envelope (`@cascade/contracts`), validated at the
 * HTTP boundary with class-validator so malformed requests get a 400. It is not
 * a second source of truth for the envelope: the Collector builds and validates
 * the full `RawEvent` against `rawEventSchema` before producing.
 *
 * Clients never supply `eventId` (server-stamped) or `receivedAt` (ingest time,
 * stamped by the Collector). Light Phase-0/1 validation only; per-project schema
 * validation and API-key checks land in later tickets.
 */
export class CollectEventDto {
  @IsString()
  @MinLength(1)
  @MaxLength(256)
  projectId!: string;

  @IsString()
  @MinLength(1)
  @MaxLength(256)
  type!: string;

  /** Event time (ISO-8601), reported by the client. Defaults to ingest time when omitted. */
  @IsOptional()
  @IsISO8601()
  occurredAt?: string;

  @IsOptional()
  @IsObject()
  payload?: Record<string, unknown>;

  /** Optional: client session this event belongs to. */
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(256)
  sessionId?: string;

  /** Optional: the player/user the event is about. */
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(256)
  actorId?: string;

  /** Optional: emitting source / SDK version, e.g. `unity-sdk@1.4.0`. */
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(256)
  source?: string;
}
