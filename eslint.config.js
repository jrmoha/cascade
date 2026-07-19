// @ts-check
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';
import globals from 'globals';

export default tseslint.config(
  // `src/generated/**` is committed ts-proto output (DO NOT EDIT); excluded so
  // lint/format never rewrites it and `proto:check`'s git-diff stays stable.
  { ignores: ['**/dist/**', '**/node_modules/**', '**/src/generated/**'] },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  eslintConfigPrettier,
  {
    files: ['**/*.ts'],
  },
  // Load-test helpers (KAN-42): plain Node ESM scripts run on the host.
  {
    files: ['infra/load/**/*.mjs'],
    languageOptions: { globals: { ...globals.node } },
  },
  // The k6 spike script runs in the k6 runtime, not Node: it exposes __ENV/__VU/
  // __ITER magic globals and imports its API from `k6/*` modules.
  {
    files: ['infra/load/**/*.js'],
    languageOptions: {
      globals: { __ENV: 'readonly', __VU: 'readonly', __ITER: 'readonly' },
    },
  },
);
