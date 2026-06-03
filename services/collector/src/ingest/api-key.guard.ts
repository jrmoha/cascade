import {
  type CanActivate,
  type ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import type { Request } from 'express';
import { ProjectSchemaClient } from './project-schema.client';

/** Header carrying the project's API key on `POST /collect`. */
export const API_KEY_HEADER = 'x-api-key';

/** Request augmented by {@link ApiKeyGuard} with the authenticated project. */
export interface AuthedRequest extends Request {
  projectId?: string;
}

/**
 * Authenticates an ingest request by its `x-api-key` header (KAN-30). Resolves
 * the owning project via {@link ProjectSchemaClient} (Redis-cached) and attaches
 * it to the request, so the rest of the pipeline trusts a server-derived
 * `projectId` rather than anything the client sent.
 *
 * - missing header → `401`
 * - unknown/revoked key → `401`
 * - Project/Schema unreachable on a cold cache → `503` (propagated from the
 *   client; fail-closed, ADR-0013).
 */
@Injectable()
export class ApiKeyGuard implements CanActivate {
  constructor(private readonly projectSchema: ProjectSchemaClient) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest<AuthedRequest>();
    const header = req.headers[API_KEY_HEADER];
    const key = Array.isArray(header) ? header[0] : header;
    if (!key) {
      throw new UnauthorizedException('Missing API key');
    }

    const projectId = await this.projectSchema.resolveProjectId(key);
    if (!projectId) {
      throw new UnauthorizedException('Invalid API key');
    }

    req.projectId = projectId;
    return true;
  }
}
