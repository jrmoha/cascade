import { describe, expect, it } from 'vitest';
import { zodToJsonSchema } from 'zod-to-json-schema';
import { collectEventSchema, rawEventSchema } from '../src/events';
import { funnelResponseSchema } from '../src/funnel';
import { retentionResponseSchema } from '../src/retention';
import {
  eventSchemaSchema,
  registerEventSchemaSchema,
  verifyKeyRequestSchema,
  verifyKeyResponseSchema,
} from '../src/project-schema';

/**
 * Contract compatibility tripwire (KAN-29 / ADR-0012). Serializing every shared
 * contract to JSON Schema and snapshotting it turns *any* change to the wire
 * surface into a red test until the snapshot is deliberately regenerated
 * (`vitest -u`). That regeneration is the checkpoint to apply the
 * additive-vs-breaking rule: an additive field is safe; a rename/remove/retype
 * is breaking and must bump a version (the Kafka envelope's `schemaVersion`, or
 * a proto field number). A breaking change therefore cannot land silently — it
 * fails CI here, satisfying the "breaking change fails the build" criterion.
 *
 * The gRPC/proto side is guarded separately by `proto:check` (regenerate +
 * git-diff) plus `tsc --build` (a removed proto field breaks the controller).
 */
describe('contract compatibility snapshot', () => {
  it('matches the committed JSON Schema for every shared contract', () => {
    const contracts = {
      rawEvent: zodToJsonSchema(rawEventSchema, 'rawEvent'),
      collectEvent: zodToJsonSchema(collectEventSchema, 'collectEvent'),
      verifyKeyRequest: zodToJsonSchema(verifyKeyRequestSchema, 'verifyKeyRequest'),
      verifyKeyResponse: zodToJsonSchema(verifyKeyResponseSchema, 'verifyKeyResponse'),
      eventSchemaRecord: zodToJsonSchema(eventSchemaSchema, 'eventSchemaRecord'),
      registerEventSchema: zodToJsonSchema(registerEventSchemaSchema, 'registerEventSchema'),
      funnelResponse: zodToJsonSchema(funnelResponseSchema, 'funnelResponse'),
      retentionResponse: zodToJsonSchema(retentionResponseSchema, 'retentionResponse'),
    };

    expect(contracts).toMatchSnapshot();
  });
});
