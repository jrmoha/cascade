import { Controller, NotFoundException } from '@nestjs/common';
import { GrpcMethod, RpcException } from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import { PROJECT_SCHEMA_GRPC_SERVICE, projectSchemaProto } from '@cascade/contracts';
import { ApiKeysService } from '../api-keys/api-keys.service';
import { SchemasService } from '../schemas/schemas.service';

/**
 * gRPC face of the Project/Schema service — the internal synchronous contract
 * the Collector calls on its ingest hot path (KAN-30). It is the one justified
 * sync dependency in the ADR-0009 §4 inventory. The REST controllers remain the
 * operator/admin surface; these RPCs serve the typed, generated contract in
 * `@cascade/contracts` (`project_schema.proto`). Both methods delegate to the
 * existing services so there is no duplicated logic.
 */
@Controller()
export class ProjectSchemaGrpcController {
  constructor(
    private readonly apiKeys: ApiKeysService,
    private readonly schemas: SchemasService,
  ) {}

  /**
   * Verify a presented key. An invalid/revoked key is data (`valid=false`), not
   * an error, so the RPC succeeds either way — mirroring the REST endpoint.
   */
  @GrpcMethod(PROJECT_SCHEMA_GRPC_SERVICE, 'VerifyKey')
  verifyKey(
    request: projectSchemaProto.VerifyKeyRequest,
  ): Promise<projectSchemaProto.VerifyKeyResponse> {
    return this.apiKeys.verify(request.key);
  }

  /**
   * Fetch a project's JSON Schema for an event type. The schema document is
   * serialized as a JSON string on the wire (proto3 has no arbitrary-object
   * type); a missing schema maps to a gRPC `NOT_FOUND` status.
   */
  @GrpcMethod(PROJECT_SCHEMA_GRPC_SERVICE, 'GetEventSchema')
  async getEventSchema(
    request: projectSchemaProto.GetEventSchemaRequest,
  ): Promise<projectSchemaProto.EventSchema> {
    try {
      const record = await this.schemas.getByType(request.projectId, request.eventType);
      return {
        id: record.id,
        projectId: record.projectId,
        eventType: record.eventType,
        jsonSchema: JSON.stringify(record.jsonSchema),
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
      };
    } catch (err) {
      if (err instanceof NotFoundException) {
        throw new RpcException({ code: status.NOT_FOUND, message: err.message });
      }
      throw err;
    }
  }
}
