import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import type { ApiKeyMetadata, IssuedApiKey, VerifyKeyResponse } from '@cascade/contracts';
import { DatabaseService } from '../db/database.service';
import { ProjectsService } from '../projects/projects.service';
import { toApiKeyMetadata } from '../common/mappers';
import { generateApiKey, hashSecret, parseApiKey, verifySecret } from './api-key.util';

@Injectable()
export class ApiKeysService {
  private readonly logger = new Logger(ApiKeysService.name);

  constructor(
    private readonly db: DatabaseService,
    private readonly projects: ProjectsService,
  ) {}

  /**
   * Issue a new key for a project. Only the argon2 hash of the secret is
   * persisted; the plaintext `key` is returned **once** and is never
   * retrievable again.
   */
  async issue(projectId: string): Promise<IssuedApiKey> {
    await this.projects.assertExists(projectId);

    const generated = generateApiKey();
    const hash = await hashSecret(generated.secret);
    const row = await this.db.apiKey.create({
      data: { projectId, prefix: generated.prefix, hash },
    });

    return { ...toApiKeyMetadata(row), key: generated.key };
  }

  /** Revoke a key. A revoked key fails {@link verify} from then on. */
  async revoke(projectId: string, keyId: string): Promise<ApiKeyMetadata> {
    const row = await this.db.apiKey.findFirst({ where: { id: keyId, projectId } });
    if (!row) {
      throw new NotFoundException(`API key ${keyId} not found for project ${projectId}`);
    }
    if (row.revokedAt) {
      return toApiKeyMetadata(row);
    }
    const updated = await this.db.apiKey.update({
      where: { id: keyId },
      data: { revokedAt: new Date() },
    });
    return toApiKeyMetadata(updated);
  }

  /**
   * Hot-path verification (Collector → Project/Schema). One indexed lookup by
   * the non-secret `prefix`, then a constant-time argon2 verify of the secret.
   * A malformed, unknown, revoked, or mismatched key all return
   * `{ valid: false }` without leaking which — and it **never throws**, so a
   * corrupted stored hash degrades to a clean rejection rather than a 500 on
   * the Collector's hot path.
   */
  async verify(key: string): Promise<VerifyKeyResponse> {
    const parsed = parseApiKey(key);
    if (!parsed) {
      return { valid: false };
    }

    const row = await this.db.apiKey.findUnique({ where: { prefix: parsed.prefix } });
    if (!row || row.revokedAt) {
      return { valid: false };
    }

    try {
      const ok = await verifySecret(row.hash, parsed.secret);
      return ok ? { valid: true, projectId: row.projectId } : { valid: false };
    } catch (err) {
      this.logger.error(
        `argon2.verify failed for prefix ${parsed.prefix} — possible hash corruption: ${(err as Error).message}`,
      );
      return { valid: false };
    }
  }
}
