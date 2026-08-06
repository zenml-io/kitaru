import type {
  ReplayResponse,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionNodeResponse,
  SessionResponse,
  SessionUpdateRequest,
  ToolLookupRequest,
  ToolLookupResponse,
} from "@zenml-io/kitaru";
import type { AdapterClient } from "@zenml-io/kitaru/adapter";

const SESSION_ID = "018f0000-0000-7000-8000-000000000201";

export class SmokeClient implements AdapterClient {
  async createSession(request: SessionCreateRequest): Promise<SessionResponse> {
    return {
      id: SESSION_ID,
      origin: request.origin,
      status: "in_progress",
    } as SessionResponse;
  }

  async getReplay(): Promise<ReplayResponse> {
    throw new Error("SmokeClient does not support replay");
  }

  async lookupToolResult(
    _replayId: string,
    _request: ToolLookupRequest,
  ): Promise<ToolLookupResponse> {
    throw new Error("SmokeClient does not support history lookup");
  }

  async updateSession(
    _sessionId: string,
    request: SessionUpdateRequest,
  ): Promise<SessionResponse> {
    return {
      id: SESSION_ID,
      origin: "recorded",
      status: request.status ?? "in_progress",
    } as SessionResponse;
  }

  async upsertSessionNodes(
    _sessionId: string,
    request: SessionNodeBatchRequest,
  ): Promise<SessionNodeResponse[]> {
    return request.nodes.map((node) => ({
      id: `018f0000-0000-7000-8001-${String(node.index + 500).padStart(12, "0")}`,
      index: node.index,
      node_type: node.node_type,
      status: node.status,
    })) as SessionNodeResponse[];
  }
}
