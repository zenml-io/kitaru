import type { AdapterClient } from "../../src/adapter/index.js";
import type {
  ReplayResponse,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionUpdateRequest,
  TaskSpecResponse,
  ToolLookupRequest,
} from "../../src/client.js";

export const SESSION_ID = "018f0000-0000-7000-8000-000000000200";
export const REPLAY_ID = "018f0000-0000-7000-8000-000000000201";
export const TASK_ID = "018f0000-0000-7000-8000-000000000203";

export interface FakeClient extends AdapterClient {
  created: SessionCreateRequest[];
  lookups: ToolLookupRequest[];
  nodes: SessionNodeBatchRequest[];
  updates: SessionUpdateRequest[];
}

export function replay(
  toolPolicy: ReplayResponse["tool_policy"] = {
    default: { type: "passthrough" },
    tools: {},
  },
): ReplayResponse {
  return {
    baseline_session_id: "018f0000-0000-7000-8000-000000000202",
    id: REPLAY_ID,
    override: null,
    status: "pending",
    tool_policy: toolPolicy,
  } as ReplayResponse;
}

export function fakeClient(
  options: {
    lookup?: (request: ToolLookupRequest) => unknown;
    replay?: ReplayResponse;
    taskInput?: unknown;
    throwOnNodes?: boolean;
    throwOnUpdate?: boolean;
  } = {},
): FakeClient {
  const created: SessionCreateRequest[] = [];
  const lookups: ToolLookupRequest[] = [];
  const nodes: SessionNodeBatchRequest[] = [];
  const updates: SessionUpdateRequest[] = [];
  return {
    created,
    lookups,
    nodes,
    updates,
    async createSession(request) {
      created.push(request);
      return {
        id: SESSION_ID,
        origin: request.origin,
        status: request.status ?? "in_progress",
      } as never;
    },
    async getReplay() {
      return options.replay ?? replay();
    },
    async getTaskSpec() {
      return {
        details: { inputs: options.taskInput, kind: "agent" },
        env: {},
        kind: "agent",
        secret_env: {},
        task_id: TASK_ID,
        timeout_seconds: 60,
      } as TaskSpecResponse;
    },
    async lookupToolResult(_replayId, request) {
      lookups.push(request);
      return (options.lookup?.(request) ?? {
        found: false,
        result: null,
      }) as never;
    },
    async updateSession(_sessionId, request) {
      updates.push(request);
      if (options.throwOnUpdate) {
        throw new Error("update cleanup failed");
      }
      return {
        id: SESSION_ID,
        origin: "recorded",
        status: request.status ?? "in_progress",
      } as never;
    },
    async upsertSessionNodes(_sessionId, request) {
      nodes.push(request);
      if (options.throwOnNodes) {
        throw new Error("node cleanup failed");
      }
      return [];
    },
  };
}
