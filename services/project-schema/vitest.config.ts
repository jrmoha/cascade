import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  // unplugin-swc (below) sets `esbuild: false` and is the sole transformer, so
  // we emit decorator metadata for NestJS DI. Disable Vite's native Oxc pass
  // too, otherwise Vite 8 warns that `esbuild: false` no longer has any effect.
  oxc: false,
  test: {
    globals: true,
    environment: 'node',
    include: ['test/**/*.{spec,e2e-spec}.ts'],
    // Spinning up a Postgres container is slow; give integration tests room.
    testTimeout: 120_000,
    hookTimeout: 240_000,
    // Integration specs each spin up a Postgres container. Run test files
    // sequentially so we don't start several heavy containers at once.
    fileParallelism: false,
  },
  plugins: [
    // Emit decorator metadata so NestJS DI works under Vitest.
    swc.vite({
      module: { type: 'es6' },
    }),
  ],
});
