import { z } from 'zod';

/**
 * Project/Schema environment contract. The only infra dependency is Postgres,
 * addressed via `DATABASE_URL` (the var Prisma reads directly). It is
 * **required** with no default — a missing/invalid value fails validation at
 * boot rather than silently falling back to localhost (12-factor). Only the
 * service's own HTTP bind port carries a conventional default. Validated once
 * at boot — see {@link AppConfigModule}.
 */
export const projectSchemaEnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3004),
  DATABASE_URL: z.string().url(),
});

export type ProjectSchemaConfig = z.infer<typeof projectSchemaEnvSchema>;
