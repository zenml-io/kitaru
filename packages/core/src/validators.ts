import type { HttpMethod } from "./transport.js";
import type {
  ReplayResponse,
  SessionNodeResponse,
  SessionResponse,
  TaskSpecResponse,
  ToolLookupResponse,
} from "./types.js";
import {
  invalidResponse,
  isRecord,
  requireResponseEnum,
  requireResponseString,
  requireResponseUuid,
} from "./validation.js";

const SESSION_ORIGINS = new Set(["imported", "recorded", "replay"]);
const SESSION_STATUSES = new Set(["in_progress", "completed", "failed"]);
const NODE_TYPES = new Set(["llm_call", "tool_call", "subagent_call", "span"]);
const NODE_STATUSES = new Set(["in_progress", "completed", "failed"]);
const REPLAY_STATUSES = new Set([
  "pending",
  "evaluating",
  "completed",
  "failed",
  "canceled",
]);
const TOOL_POLICY_TYPES = new Set(["history", "llm", "passthrough", "static"]);
const TOOL_POLICY_ON_MISS = new Set(["error_result", "fail", "passthrough"]);
const HISTORY_SCOPES = new Set(["agent", "baseline", "cohort_version"]);
const TASK_KINDS = new Set(["agent", "evaluator", "importer"]);

function requireString(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  requireResponseString(
    value,
    property,
    method,
    path,
    status,
    `missing ${property}`,
  );
}

function requireDiscriminator(
  value: Record<string, unknown>,
  property: string,
  allowed: ReadonlySet<string>,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  requireResponseEnum(
    value,
    property,
    allowed,
    method,
    path,
    status,
    `missing ${property}`,
  );
}

function requireId(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  requireResponseUuid(
    value,
    property,
    method,
    path,
    status,
    `missing ${property}`,
  );
}

export function validateSession(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is SessionResponse {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, "expected a session object");
  }
  requireId(value, "id", method, path, status);
  requireDiscriminator(value, "origin", SESSION_ORIGINS, method, path, status);
  requireDiscriminator(value, "status", SESSION_STATUSES, method, path, status);
}

export function validateNode(
  node: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts node is SessionNodeResponse {
  if (!isRecord(node)) {
    invalidResponse(method, path, status, "expected a node object");
  }
  requireId(node, "id", method, path, status);
  requireDiscriminator(node, "node_type", NODE_TYPES, method, path, status);
  requireDiscriminator(node, "status", NODE_STATUSES, method, path, status);
}

export function validateNodes(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is SessionNodeResponse[] {
  if (!Array.isArray(value)) {
    invalidResponse(method, path, status, "expected a node array");
  }
  for (const node of value) {
    validateNode(node, method, path, status);
  }
}

function validateToolPolicyConfig(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, "invalid tool policy config");
  }
  requireDiscriminator(value, "type", TOOL_POLICY_TYPES, method, path, status);
  if (value.type === "history") {
    requireDiscriminator(value, "scope", HISTORY_SCOPES, method, path, status);
    requireDiscriminator(
      value,
      "on_miss",
      TOOL_POLICY_ON_MISS,
      method,
      path,
      status,
    );
  } else if (value.type === "static") {
    if (!Array.isArray(value.cases)) {
      invalidResponse(method, path, status, "missing static policy cases");
    }
    requireDiscriminator(
      value,
      "on_miss",
      TOOL_POLICY_ON_MISS,
      method,
      path,
      status,
    );
  } else if (value.type === "llm") {
    requireString(value, "model", method, path, status);
  }
}

function validateToolPolicy(
  value: Record<string, unknown>,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  validateToolPolicyConfig(value.default, method, path, status);
  if (value.tools === undefined) {
    return;
  }
  if (!isRecord(value.tools)) {
    invalidResponse(method, path, status, "invalid tool policy overrides");
  }
  for (const config of Object.values(value.tools)) {
    validateToolPolicyConfig(config, method, path, status);
  }
}

export function validateReplay(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ReplayResponse {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, "expected a replay object");
  }
  requireId(value, "id", method, path, status);
  requireId(value, "job_id", method, path, status);
  requireId(value, "baseline_session_id", method, path, status);
  requireDiscriminator(value, "status", REPLAY_STATUSES, method, path, status);
  if (!isRecord(value.tool_policy)) {
    invalidResponse(method, path, status, "missing tool_policy");
  }
  validateToolPolicy(value.tool_policy, method, path, status);
}

export function validateTaskSpec(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is TaskSpecResponse {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, "expected a task spec object");
  }
  requireId(value, "task_id", method, path, status);
  requireDiscriminator(value, "kind", TASK_KINDS, method, path, status);
  if (
    !Number.isFinite(value.timeout_seconds) ||
    !isRecord(value.env) ||
    !isRecord(value.secret_env) ||
    !isRecord(value.details) ||
    value.details.kind !== value.kind
  ) {
    invalidResponse(method, path, status, "missing task details");
  }
  if (
    value.kind === "agent" &&
    value.details.kind === "agent" &&
    !Object.hasOwn(value.details, "inputs")
  ) {
    invalidResponse(method, path, status, "missing task inputs");
  }
}

export function validateToolLookup(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ToolLookupResponse {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, "invalid response");
  }
  if (!Object.hasOwn(value, "match") || value.match === null) {
    return;
  }
  if (!isRecord(value.match)) {
    invalidResponse(method, path, status, "invalid match");
  }
  if (!Object.hasOwn(value.match, "result")) {
    invalidResponse(method, path, status, "missing result");
  }
  requireDiscriminator(
    value.match,
    "status",
    NODE_STATUSES,
    method,
    path,
    status,
  );
  if (
    value.match.error !== undefined &&
    value.match.error !== null &&
    typeof value.match.error !== "string"
  ) {
    invalidResponse(method, path, status, "invalid error");
  }
}
