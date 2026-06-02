import { randomBytes } from 'node:crypto';
import * as argon2 from 'argon2';
import { API_KEY_ID_LENGTH, API_KEY_PREFIX, API_KEY_SECRET_LENGTH } from '@cascade/contracts';

/** A freshly generated key: the plaintext to hand back once, plus its parts. */
export interface GeneratedApiKey {
  /** Non-secret lookup id, e.g. `cas_a1b2c3d4`. Stored and indexed. */
  prefix: string;
  /** Secret half — hashed, never stored in the clear. */
  secret: string;
  /** Full plaintext key `cas_<id>.<secret>` returned to the caller once. */
  key: string;
}

/** The two halves of a presented key, split on the first `.`. */
export interface ParsedApiKey {
  prefix: string;
  secret: string;
}

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

/**
 * Cryptographically-random base62 string of the given length.
 *
 * Note the modulo bias: `256 % 62 = 8`, so the first 8 alphabet chars appear at
 * `5/256` vs `4/256` per byte. For a 32-char secret that's ~190 effective bits
 * vs a theoretical 192 — a negligible reduction, not worth rejection-sampling.
 */
function randomString(length: number): string {
  const bytes = randomBytes(length);
  let out = '';
  for (let i = 0; i < length; i++) {
    out += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return out;
}

/** Mint a new API key (`cas_<id8>.<secret32>`). */
export function generateApiKey(): GeneratedApiKey {
  const prefix = `${API_KEY_PREFIX}${randomString(API_KEY_ID_LENGTH)}`;
  const secret = randomString(API_KEY_SECRET_LENGTH);
  return { prefix, secret, key: `${prefix}.${secret}` };
}

/** argon2-hash a key's secret half (stored in `api_keys.hash`). */
export function hashSecret(secret: string): Promise<string> {
  return argon2.hash(secret);
}

/** Constant-time verify of a presented secret against a stored argon2 hash. */
export function verifySecret(hash: string, secret: string): Promise<boolean> {
  return argon2.verify(hash, secret);
}

/**
 * Split a presented key into `{ prefix, secret }`. Returns `null` for anything
 * not shaped like `cas_<id>.<secret>`, so verify can fail fast without a lookup.
 */
export function parseApiKey(key: string): ParsedApiKey | null {
  const dot = key.indexOf('.');
  if (dot <= 0 || dot === key.length - 1) {
    return null;
  }
  const prefix = key.slice(0, dot);
  const secret = key.slice(dot + 1);
  if (!prefix.startsWith(API_KEY_PREFIX)) {
    return null;
  }
  return { prefix, secret };
}
