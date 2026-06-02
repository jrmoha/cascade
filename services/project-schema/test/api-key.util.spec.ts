import { describe, expect, it } from 'vitest';
import { API_KEY_PREFIX } from '@cascade/contracts';
import {
  generateApiKey,
  hashSecret,
  parseApiKey,
  verifySecret,
} from '../src/api-keys/api-key.util';

describe('api-key util', () => {
  it('mints a key shaped cas_<id>.<secret> with matching prefix', () => {
    const { key, prefix, secret } = generateApiKey();
    expect(prefix.startsWith(API_KEY_PREFIX)).toBe(true);
    expect(key).toBe(`${prefix}.${secret}`);
    expect(parseApiKey(key)).toEqual({ prefix, secret });
  });

  it('generates distinct keys', () => {
    expect(generateApiKey().key).not.toBe(generateApiKey().key);
  });

  it('hashes the secret and verifies the round-trip', async () => {
    const { secret } = generateApiKey();
    const hash = await hashSecret(secret);
    expect(hash).not.toContain(secret);
    expect(await verifySecret(hash, secret)).toBe(true);
    expect(await verifySecret(hash, 'wrong-secret')).toBe(false);
  });

  it('rejects malformed keys', () => {
    expect(parseApiKey('no-dot')).toBeNull();
    expect(parseApiKey('.secret')).toBeNull();
    expect(parseApiKey('cas_abc.')).toBeNull();
    expect(parseApiKey('nope_abc.secret')).toBeNull();
  });
});
