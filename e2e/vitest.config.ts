import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

// The monorepo root (this package's parent), used to widen Vite's fs allow-list.
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

export default defineConfig({
  // unplugin-swc (below) sets `esbuild: false` and is the sole transformer, so
  // we emit decorator metadata for NestJS DI. Disable Vite's native Oxc pass
  // too, otherwise Vite 8 warns that `esbuild: false` no longer has any effect.
  oxc: false,
  server: {
    fs: {
      // This smoke test imports the three services' source straight from their
      // sibling workspaces, which live outside this package. Allow Vite to read
      // the whole monorepo so those imports resolve.
      allow: [repoRoot],
    },
  },
  test: {
    globals: true,
    environment: 'node',
    include: ['test/**/*.{spec,e2e-spec}.ts'],
    // Two containers (Kafka + Cassandra) plus three services booting in-process,
    // then an async pipe to drain — give the whole gate generous headroom.
    testTimeout: 240_000,
    hookTimeout: 300_000,
  },
  plugins: [
    // Emit decorator metadata so NestJS DI works under Vitest.
    swc.vite({
      module: { type: 'es6' },
    }),
  ],
});
