import { join } from 'node:path';

/**
 * Runtime wiring constants for the Project/Schema gRPC contract (KAN-29). The
 * `.proto` is the contract; NestJS + `@grpc/proto-loader` load it at boot for
 * (de)serialization, so both the server (Project/Schema) and the client
 * (Collector, KAN-30) reference the same file and package name from here.
 */

/** The proto `package` — also the NestJS gRPC transport `package` option. */
export const PROJECT_SCHEMA_PROTO_PACKAGE = 'cascade.projectschema.v1';

/** The proto `service` name, used in `@GrpcMethod(<service>, <method>)`. */
export const PROJECT_SCHEMA_GRPC_SERVICE = 'ProjectSchema';

/**
 * Absolute path to `project_schema.proto`. Resolves relative to this compiled
 * file: `dist/grpc.js` → `../proto/project_schema.proto`, so it works both for
 * the symlinked workspace (dev/tests) and the container image (the Dockerfile
 * copies `libs/contracts/proto` alongside `dist`).
 */
export const PROJECT_SCHEMA_PROTO_PATH = join(__dirname, '..', 'proto', 'project_schema.proto');
