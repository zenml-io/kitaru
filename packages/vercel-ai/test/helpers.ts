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

export const AGENT_ID = "018f0000-0000-7000-8000-000000000100";
export const REPLAY_ID = "018f0000-0000-7000-8000-000000000101";
export const SESSION_ID = "018f0000-0000-7000-8000-000000000102";

export const TEST_USAGE = {
  inputTokens: {
    cacheRead: undefined,
    cacheWrite: undefined,
    noCache: 3,
    total: 3,
  },
  outputTokens: { reasoning: undefined, text: 2, total: 2 },
};

export function textResponse(text = "done") {
  return {
    content: [{ text, type: "text" as const }],
    finishReason: { raw: "stop", unified: "stop" as const },
    usage: TEST_USAGE,
    warnings: [],
  };
}

export function toolResponse(
  calls: Array<{ id: string; input?: string; name: string }>,
) {
  return {
    content: calls.map((call) => ({
      input: call.input ?? "{}",
      toolCallId: call.id,
      toolName: call.name,
      type: "tool-call" as const,
    })),
    finishReason: { raw: "tool_calls", unified: "tool-calls" as const },
    usage: TEST_USAGE,
    warnings: [],
  };
}

export interface FakeClientOptions {
  failNodeBatch?: (batch: SessionNodeBatchRequest, index: number) => boolean;
  lookup?: (request: ToolLookupRequest) => ToolLookupResponse;
  replay?: ReplayResponse;
  updateFails?: boolean;
}

export class FakeClient implements AdapterClient {
  readonly created: SessionCreateRequest[] = [];
  readonly lookups: ToolLookupRequest[] = [];
  readonly nodeBatches: SessionNodeBatchRequest[] = [];
  readonly updated: SessionUpdateRequest[] = [];
  replayReads = 0;

  readonly #options: FakeClientOptions;

  constructor(options: FakeClientOptions = {}) {
    this.#options = options;
  }

  async createSession(request: SessionCreateRequest): Promise<SessionResponse> {
    this.created.push(request);
    return {
      id: SESSION_ID,
      origin: request.origin,
      status: "in_progress",
    } as SessionResponse;
  }

  async getReplay(): Promise<ReplayResponse> {
    this.replayReads += 1;
    return this.#options.replay ?? replaySpec();
  }

  async lookupToolResult(
    _replayId: string,
    request: ToolLookupRequest,
  ): Promise<ToolLookupResponse> {
    this.lookups.push(request);
    return this.#options.lookup?.(request) ?? { found: false, result: null };
  }

  async updateSession(
    _sessionId: string,
    request: SessionUpdateRequest,
  ): Promise<SessionResponse> {
    this.updated.push(request);
    if (this.#options.updateFails) {
      throw new Error("cleanup failed");
    }
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
    const index = this.nodeBatches.length;
    this.nodeBatches.push(request);
    if (this.#options.failNodeBatch?.(request, index)) {
      throw new Error("node upload failed");
    }
    return request.nodes.map((node) => ({
      id: `018f0000-0000-7000-8001-${String(node.index + 300).padStart(12, "0")}`,
      index: node.index,
      node_type: node.node_type,
      status: node.status,
    })) as SessionNodeResponse[];
  }
}

export function replaySpec(
  defaultPolicy: Record<string, unknown> = { type: "passthrough" },
  override: Record<string, unknown> | null = null,
): ReplayResponse {
  return {
    baseline_session_id: "018f0000-0000-7000-8000-000000000103",
    id: REPLAY_ID,
    override,
    status: "pending",
    tool_policy: { default: defaultPolicy, tools: {} },
  } as ReplayResponse;
}

export function replayEnvironment(
  extra: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  return { KITARU_REPLAY_ID: REPLAY_ID, ...extra };
}
