import { Body, Controller, HttpCode, HttpStatus, Param, ParseUUIDPipe, Post } from '@nestjs/common';
import {
  type ApiKeyMetadata,
  type IssuedApiKey,
  type VerifyKeyRequest,
  type VerifyKeyResponse,
  verifyKeyRequestSchema,
} from '@cascade/contracts';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { ApiKeysService } from './api-keys.service';

@Controller()
export class ApiKeysController {
  constructor(private readonly apiKeys: ApiKeysService) {}

  /** Issue a new API key for a project. Returns the plaintext key **once**. */
  @Post('projects/:projectId/keys')
  @HttpCode(HttpStatus.CREATED)
  issue(@Param('projectId', ParseUUIDPipe) projectId: string): Promise<IssuedApiKey> {
    return this.apiKeys.issue(projectId);
  }

  /** Revoke a key. Subsequent verifications of it fail. */
  @Post('projects/:projectId/keys/:keyId/revoke')
  @HttpCode(HttpStatus.OK)
  revoke(
    @Param('projectId', ParseUUIDPipe) projectId: string,
    @Param('keyId', ParseUUIDPipe) keyId: string,
  ): Promise<ApiKeyMetadata> {
    return this.apiKeys.revoke(projectId, keyId);
  }

  /**
   * Verify a presented key (hot path; Collector consumes this in KAN-30).
   * Always `200` with `{ valid, projectId? }` — an invalid key is data, not an
   * error.
   */
  @Post('api-keys/verify')
  @HttpCode(HttpStatus.OK)
  verify(
    @Body(new ZodValidationPipe(verifyKeyRequestSchema)) body: VerifyKeyRequest,
  ): Promise<VerifyKeyResponse> {
    return this.apiKeys.verify(body.key);
  }
}
