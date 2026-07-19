import {
  type CanActivate,
  type ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import { API_KEY_HEADER } from './api-key.guard';
import { RateLimiter } from './rate-limit';

/**
 * Per-API-key rate limiting at the ingest edge (KAN-42, ADR-0021). Runs
 * **before** {@link ApiKeyGuard} so a flood on one key is capped before it
 * reaches auth or the Project/Schema dependency.
 *
 * - no `x-api-key` header → passes through (ApiKeyGuard then returns the 401;
 *   the limiter has no key to bucket on);
 * - over budget → `429 Too Many Requests` with a `Retry-After` header;
 * - Redis unreachable → the limiter {@link RateLimiter.consume | fails open}.
 */
@Injectable()
export class RateLimitGuard implements CanActivate {
  constructor(private readonly limiter: RateLimiter) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest<Request>();
    const header = req.headers[API_KEY_HEADER];
    const key = Array.isArray(header) ? header[0] : header;
    if (!key) {
      return true;
    }

    const { allowed, retryAfterMs } = await this.limiter.consume(key);
    if (allowed) {
      return true;
    }

    const retryAfterSec = Math.max(1, Math.ceil(retryAfterMs / 1000));
    context.switchToHttp().getResponse<Response>().setHeader('Retry-After', retryAfterSec);
    throw new HttpException(
      {
        statusCode: HttpStatus.TOO_MANY_REQUESTS,
        error: 'Too Many Requests',
        message: 'Rate limit exceeded for this API key',
        retryAfterSeconds: retryAfterSec,
      },
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }
}
