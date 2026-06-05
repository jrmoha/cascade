import { createParamDecorator, type ExecutionContext } from '@nestjs/common';
import type { AuthedRequest } from './api-key.guard';

/**
 * Injects the `projectId` that {@link ApiKeyGuard} resolved from the API key.
 * The guard must run before the handler (it does — guards precede pipes), so a
 * missing value here is a wiring bug rather than a client error.
 */
export const ProjectId = createParamDecorator((_data: unknown, ctx: ExecutionContext): string => {
  const req = ctx.switchToHttp().getRequest<AuthedRequest>();
  if (!req.projectId) {
    throw new Error('ProjectId used without ApiKeyGuard');
  }
  return req.projectId;
});
