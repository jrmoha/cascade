import { z } from 'zod';

/**
 * Project/Schema environment contract. The only infra dependency is Postgres,
 * addressed via `DATABASE_URL` (the var Prisma reads directly). It is
 * **required** with no default — a missing/invalid value fails validation at
 * boot rather than silently falling back to localhost (12-factor). Only the
 * service's own bind addresses (HTTP `PORT`, gRPC `GRPC_URL`) carry conventional
 * defaults. Validated once at boot — see {@link AppConfigModule}.
 */
export const projectSchemaEnvSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3004),
  DATABASE_URL: z.string().url(),
  /**
   * Bind address for the gRPC microservice (the internal sync contract the
   * Collector calls — KAN-29/30). Own bind address, so it defaults; override in
   * containers to the in-network host:port.
   */
  GRPC_URL: z.string().min(1).default('0.0.0.0:50051'),
});

export type ProjectSchemaConfig = z.infer<typeof projectSchemaEnvSchema>;
