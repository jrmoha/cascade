import { IsObject, IsOptional, IsString, MaxLength, MinLength } from 'class-validator';

/**
 * Light, Phase-0 validation only. Real schema validation (per-project event
 * schemas, API-key checks) lands in Phase 1.
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

  /** ISO-8601 string. Defaulted to ingestion time when omitted. */
  @IsOptional()
  @IsString()
  timestamp?: string;

  @IsOptional()
  @IsObject()
  payload?: Record<string, unknown>;
}
