import type {
  ReplayResponse,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionNodeResponse,
  SessionResponse,
  SessionUpdateRequest,
  TaskSpecResponse,
  ToolLookupRequest,
  ToolLookupResponse,
} from "@zenml-io/kitaru";
import type { AdapterClient } from "@zenml-io/kitaru/adapter";

function sessionId(index: number): string {
  return `018f0000-0000-7000-8000-${String(index + 201).padStart(12, "0")}`;
}

export class SmokeClient implements AdapterClient {
  readonly created: SessionCreateRequest[] = [];
  readonly nodeBatches: SessionNodeBatchRequest[] = [];
  readonly updated: SessionUpdateRequest[] = [];

  async createSession(request: SessionCreateRequest): Promise<SessionResponse> {
    const index = this.created.push(request) - 1;
    const id = sessionId(index);
    return {
      id,
      origin: request.origin,
      status: "in_progress",
    } as SessionResponse;
  }

  async getReplay(): Promise<ReplayResponse> {
    throw new Error("SmokeClient does not support replay metadata");
  }

  async getTaskSpec(): Promise<TaskSpecResponse> {
    throw new Error("SmokeClient does not support task execution");
  }

  async lookupToolResult(
    _replayId: string,
    _request: ToolLookupRequest,
  ): Promise<ToolLookupResponse> {
    throw new Error("SmokeClient does not support history lookup");
  }

  async updateSession(
    sessionIdValue: string,
    request: SessionUpdateRequest,
  ): Promise<SessionResponse> {
    this.updated.push(request);
    return {
      id: sessionIdValue,
      origin: "recorded",
      status: request.status ?? "in_progress",
    } as SessionResponse;
  }

  async upsertSessionNodes(
    _sessionId: string,
    request: SessionNodeBatchRequest,
  ): Promise<SessionNodeResponse[]> {
    const offset = this.nodeBatches.reduce(
      (total, batch) => total + batch.nodes.length,
      0,
    );
    this.nodeBatches.push(request);
    return request.nodes.map((node, position) => ({
      id: `018f0000-0000-7000-8001-${String(offset + position + 500).padStart(12, "0")}`,
      external_id: node.external_id,
      node_type: node.node_type,
      status: node.status,
    })) as SessionNodeResponse[];
  }
}
