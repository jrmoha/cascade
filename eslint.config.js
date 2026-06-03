// @ts-check
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';

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
);
