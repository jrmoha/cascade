export * from './events';
export * from './dead-letter';
export * from './time-window';
export * from './leaderboard';
export * from './funnel';
export * from './retention';
export * from './counts';
export * from './project-schema';
export * from './grpc';

/**
 * The gRPC sync contract (Collector → Project/Schema), generated from
 * `proto/project_schema.proto` by `npm run proto:gen` (KAN-29). Re-exported
 * under a namespace because its message interfaces share names with the Zod
 * wire types (e.g. `VerifyKeyRequest`): the Zod types are the REST/validation
 * contract; `projectSchemaProto` is the gRPC contract. Both are generated from
 * a single source of truth, so neither side hand-copies types.
 */
export * as projectSchemaProto from './generated/project_schema';
