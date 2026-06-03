import { ServiceUnavailableException, UnauthorizedException } from '@nestjs/common';
import type { ExecutionContext } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';
import { ApiKeyGuard, type AuthedRequest } from '../src/ingest/api-key.guard';
import type { ProjectSchemaClient } from '../src/ingest/project-schema.client';

function contextFor(req: Partial<AuthedRequest>): { ctx: ExecutionContext; req: AuthedRequest } {
  const request = { headers: {}, ...req } as AuthedRequest;
  const ctx = {
    switchToHttp: () => ({ getRequest: () => request }),
  } as unknown as ExecutionContext;
  return { ctx, req: request };
}

function guardWith(resolve: ReturnType<typeof vi.fn>): ApiKeyGuard {
  const client = { resolveProjectId: resolve } as unknown as ProjectSchemaClient;
  return new ApiKeyGuard(client);
}

describe('ApiKeyGuard', () => {
  it('rejects a request with no x-api-key header (401)', async () => {
    const guard = guardWith(vi.fn());
    const { ctx } = contextFor({ headers: {} });
    await expect(guard.canActivate(ctx)).rejects.toBeInstanceOf(UnauthorizedException);
  });

  it('attaches the resolved projectId and allows a valid key', async () => {
    const resolve = vi.fn().mockResolvedValue('proj-1');
    const guard = guardWith(resolve);
    const { ctx, req } = contextFor({ headers: { 'x-api-key': 'cas_a.secret' } });

    await expect(guard.canActivate(ctx)).resolves.toBe(true);
    expect(resolve).toHaveBeenCalledWith('cas_a.secret');
    expect(req.projectId).toBe('proj-1');
  });

  it('rejects an unknown/revoked key (401)', async () => {
    const guard = guardWith(vi.fn().mockResolvedValue(null));
    const { ctx } = contextFor({ headers: { 'x-api-key': 'cas_bad.key' } });
    await expect(guard.canActivate(ctx)).rejects.toBeInstanceOf(UnauthorizedException);
  });

  it('propagates a 503 when key resolution fails closed', async () => {
    const guard = guardWith(vi.fn().mockRejectedValue(new ServiceUnavailableException()));
    const { ctx } = contextFor({ headers: { 'x-api-key': 'cas_a.secret' } });
    await expect(guard.canActivate(ctx)).rejects.toBeInstanceOf(ServiceUnavailableException);
  });
});
