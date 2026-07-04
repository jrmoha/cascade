import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * CQRS read-boundary guardrail (KAN-36, ADR-0018). This turns the "analytics is
 * served only from derived views, never from raw storage" rule from prose into
 * an enforced check. The Query API's *one* raw capability is the bounded
 * retrieval of ADR-0008 (`GET /query`, the `query/` module); every analytics
 * endpoint (counts, leaderboard, funnel, retention) must read a pre-aggregated
 * read model and never touch the raw write path (`raw_events`).
 *
 * The boundary is specifically about the raw write path, NOT Cassandra in
 * general: the counts view lives in Cassandra `counter` aggregate tables and is
 * a legitimate derived-view read (ADR-0015 §2). So the check targets the
 * `raw_events` table name and the `RawEventReadRepository` — not the driver.
 *
 * If this test fails because an analytics module started reading raw rows, the
 * fix is to add the missing derived view to the Aggregator, not to relax the
 * check — that is exactly the design smell the boundary exists to catch.
 */

const SRC = join(import.meta.dirname, '..', 'src');
const ANALYTICS_DIRS = ['counts', 'leaderboard', 'funnel', 'retention'];
const RAW_MARKERS = /raw_events|RawEventReadRepository/;

/** Strip block + line comments so the guardrail inspects executable code, not
 * prose — the docs that explain the boundary legitimately name `raw_events`
 * ("served from the aggregate table, never `raw_events`") and must not trip it. */
function stripComments(src: string): string {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
}

/** Recursively collect `[relativePath, code]` (comments stripped) for every
 * `.ts` under `dir`. */
function readTsFiles(dir: string): Array<[string, string]> {
  const out: Array<[string, string]> = [];
  const walk = (current: string): void => {
    for (const entry of readdirSync(current)) {
      const full = join(current, entry);
      if (statSync(full).isDirectory()) walk(full);
      else if (entry.endsWith('.ts')) {
        out.push([full.slice(SRC.length + 1), stripComments(readFileSync(full, 'utf8'))]);
      }
    }
  };
  walk(dir);
  return out;
}

describe('CQRS read boundary (ADR-0018)', () => {
  it.each(ANALYTICS_DIRS)('analytics module "%s" never reads the raw write path', (dir) => {
    for (const [path, contents] of readTsFiles(join(SRC, dir))) {
      expect(RAW_MARKERS.test(contents), `${path} references the raw write path`).toBe(false);
    }
  });

  it('the raw_events table is referenced only under src/query/', () => {
    const offenders = readTsFiles(SRC)
      .filter(([, contents]) => /raw_events/.test(contents))
      .map(([path]) => path)
      .filter((path) => !path.startsWith('query/'));

    expect(offenders, `raw_events referenced outside query/: ${offenders.join(', ')}`).toEqual([]);
  });
});
