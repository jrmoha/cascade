import { BadRequestException, Injectable, PipeTransform } from '@nestjs/common';
import type { ZodSchema } from 'zod';

/** One failing field in a validation error response. */
export interface FieldError {
  /** Dotted path to the offending field (`(root)` for the whole body). */
  field: string;
  /** Human-readable reason the field failed. */
  reason: string;
}

/**
 * Validates a request value against a Zod schema and returns the parsed data.
 * On failure it throws a `400` with a **structured** body listing every failing
 * field and why, so a client can act on it programmatically. Schemas are derived
 * from the shared contract (`@cascade/contracts`), so the HTTP edge validates
 * against the one canonical definition — no re-implemented copy.
 */
@Injectable()
export class ZodValidationPipe<T> implements PipeTransform<unknown, T> {
  constructor(private readonly schema: ZodSchema<T>) {}

  transform(value: unknown): T {
    const result = this.schema.safeParse(value);
    if (result.success) {
      return result.data;
    }

    const errors: FieldError[] = result.error.issues.map((issue) => ({
      field: issue.path.join('.') || '(root)',
      reason: issue.message,
    }));

    throw new BadRequestException({
      statusCode: 400,
      error: 'Bad Request',
      message: 'Validation failed',
      errors,
    });
  }
}
