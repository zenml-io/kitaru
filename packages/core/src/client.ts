import type {
  KitaruEnvironment,
  KitaruEnvironmentOptions,
} from "./environment.js";
import { resolveKitaruEnvironment } from "./environment.js";
import { KitaruApiError } from "./errors.js";
import type { components } from "./generated/openapi.js";

export type SessionCreateRequest =
  components["schemas"]["SessionCreateRequest"];
export type SessionUpdateRequest =
  components["schemas"]["SessionUpdateRequest"];
export type SessionNodeBatchRequest =
  components["schemas"]["SessionNodeBatchRequest"];
export type SessionResponse = components["schemas"]["SessionResponse"];
export type SessionNodeResponse = components["schemas"]["SessionNodeResponse"];
export type ReplayResponse = components["schemas"]["ReplayResponse"];
export type TaskSpecResponse = components["schemas"]["TaskSpecResponse"];
export type ToolLookupRequest = components["schemas"]["ToolLookupRequest"];
export type ToolLookupResponse = components["schemas"]["ToolLookupResponse"];

export interface KitaruClientOptions extends KitaruEnvironmentOptions {
  fetch?: typeof globalThis.fetch;
}

type HttpMethod = "GET" | "PATCH" | "POST";

const RETRYABLE_UPSERT_STATUSES = new Set([502, 503, 504]);
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validationError(
  method: HttpMethod,
  path: string,
  status: number,
  detail: string,
): never {
  throw new KitaruApiError(method, path, status, `Invalid response: ${detail}`);
}

function requireString(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  if (typeof value[property] !== "string" || value[property].length === 0) {
    validationError(method, path, status, `missing ${property}`);
  }
}

function requireDiscriminator(
  value: Record<string, unknown>,
  property: string,
  allowed: ReadonlySet<string>,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  requireString(value, property, method, path, status);
  if (!allowed.has(value[property] as string)) {
    validationError(method, path, status, `invalid ${property}`);
  }
}

function requireId(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  requireString(value, property, method, path, status);
  if (!UUID_PATTERN.test(value[property] as string)) {
    validationError(method, path, status, `invalid ${property}`);
  }
}

function validateSession(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is SessionResponse {
  if (!isRecord(value)) {
    validationError(method, path, status, "expected a session object");
  }
  requireId(value, "id", method, path, status);
  requireDiscriminator(value, "origin", SESSION_ORIGINS, method, path, status);
  requireDiscriminator(value, "status", SESSION_STATUSES, method, path, status);
}

function validateNodes(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is SessionNodeResponse[] {
  if (!Array.isArray(value)) {
    validationError(method, path, status, "expected a node array");
  }
  for (const node of value) {
    if (!isRecord(node)) {
      validationError(method, path, status, "expected a node object");
    }
    requireId(node, "id", method, path, status);
    requireDiscriminator(node, "node_type", NODE_TYPES, method, path, status);
    requireDiscriminator(node, "status", NODE_STATUSES, method, path, status);
  }
}

function validateReplay(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ReplayResponse {
  if (!isRecord(value)) {
    validationError(method, path, status, "expected a replay object");
  }
  requireId(value, "id", method, path, status);
  requireId(value, "baseline_session_id", method, path, status);
  requireDiscriminator(value, "status", REPLAY_STATUSES, method, path, status);
  if (!isRecord(value.tool_policy)) {
    validationError(method, path, status, "missing tool_policy");
  }
  validateToolPolicy(value.tool_policy, method, path, status);
}

function validateTaskSpec(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is TaskSpecResponse {
  if (!isRecord(value)) {
    validationError(method, path, status, "expected a task spec object");
  }
  requireId(value, "task_id", method, path, status);
  requireString(value, "kind", method, path, status);
  if (!isRecord(value.details) || typeof value.details.kind !== "string") {
    validationError(method, path, status, "missing task details");
  }
  if (
    value.kind === "agent" &&
    value.details.kind === "agent" &&
    !Object.hasOwn(value.details, "inputs")
  ) {
    validationError(method, path, status, "missing task inputs");
  }
}

function validateToolPolicyConfig(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  if (!isRecord(value)) {
    validationError(method, path, status, "invalid tool policy config");
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
      validationError(method, path, status, "missing static policy cases");
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
    validationError(method, path, status, "invalid tool policy overrides");
  }
  for (const config of Object.values(value.tools)) {
    validateToolPolicyConfig(config, method, path, status);
  }
}

function validateToolLookup(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ToolLookupResponse {
  if (!isRecord(value) || typeof value.found !== "boolean") {
    validationError(method, path, status, "missing found discriminator");
  }
  if (!Object.hasOwn(value, "result")) {
    validationError(method, path, status, "missing result");
  }
}

function safeErrorDetail(payload: unknown, fallback: string): string {
  if (!isRecord(payload) || !Object.hasOwn(payload, "detail")) {
    return fallback;
  }
  if (typeof payload.detail === "string") {
    return fallback;
  }
  if (!Array.isArray(payload.detail)) {
    return fallback;
  }

  const messages = payload.detail.flatMap((entry) => {
    if (!isRecord(entry) || typeof entry.msg !== "string") {
      return [];
    }
    const location = Array.isArray(entry.loc)
      ? entry.loc.filter((item) => typeof item === "string").join(".")
      : "";
    return [location ? `${location}: ${entry.msg}` : entry.msg];
  });
  return messages.length > 0 ? messages.join("; ") : fallback;
}

export class KitaruClient {
  readonly #environment: KitaruEnvironment;
  readonly #fetch: typeof globalThis.fetch;

  constructor(options: KitaruClientOptions = {}) {
    this.#environment = resolveKitaruEnvironment(options);
    this.#fetch = options.fetch ?? globalThis.fetch;
  }

  async createSession(request: SessionCreateRequest): Promise<SessionResponse> {
    const method = "POST";
    const path = "/v1/sessions";
    const response = await this.#request(method, path, request, false);
    validateSession(response.value, method, path, response.status);
    return response.value;
  }

  async updateSession(
    sessionId: string,
    request: SessionUpdateRequest,
  ): Promise<SessionResponse> {
    const method = "PATCH";
    const path = `/v1/sessions/${encodeURIComponent(sessionId)}`;
    const response = await this.#request(method, path, request, false);
    validateSession(response.value, method, path, response.status);
    return response.value;
  }

  async upsertSessionNodes(
    sessionId: string,
    request: SessionNodeBatchRequest,
  ): Promise<SessionNodeResponse[]> {
    const method = "POST";
    const path = `/v1/sessions/${encodeURIComponent(sessionId)}/nodes`;
    const response = await this.#request(method, path, request, true);
    validateNodes(response.value, method, path, response.status);
    return response.value;
  }

  async getReplay(replayId: string): Promise<ReplayResponse> {
    const method = "GET";
    const path = `/v1/replays/${encodeURIComponent(replayId)}`;
    const response = await this.#request(method, path, undefined, false);
    validateReplay(response.value, method, path, response.status);
    return response.value;
  }

  async getTaskSpec(taskId: string): Promise<TaskSpecResponse> {
    const method = "GET";
    const path = `/v1/tasks/${encodeURIComponent(taskId)}/spec`;
    const response = await this.#request(method, path, undefined, false);
    validateTaskSpec(response.value, method, path, response.status);
    return response.value;
  }

  async lookupToolResult(
    replayId: string,
    request: ToolLookupRequest,
  ): Promise<ToolLookupResponse> {
    const method = "POST";
    const path = `/v1/replays/${encodeURIComponent(replayId)}/tool-lookup`;
    const response = await this.#request(method, path, request, false);
    validateToolLookup(response.value, method, path, response.status);
    return response.value;
  }

  async #request(
    method: HttpMethod,
    path: string,
    request: unknown,
    retryUpsert: boolean,
  ): Promise<{ status: number; value: unknown }> {
    const body = request === undefined ? undefined : JSON.stringify(request);
    const attempts = retryUpsert ? 2 : 1;

    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort(),
        this.#environment.timeoutMs,
      );
      let response: Response;
      try {
        response = await this.#fetch(`${this.#environment.apiUrl}${path}`, {
          method,
          headers: {
            Accept: "application/json",
            ...(body === undefined
              ? {}
              : { "Content-Type": "application/json" }),
            ...(this.#environment.apiKey === undefined
              ? {}
              : { Authorization: `Bearer ${this.#environment.apiKey}` }),
          },
          body,
          signal: controller.signal,
        });
      } catch (error) {
        clearTimeout(timeout);
        if (attempt + 1 < attempts) {
          continue;
        }
        const detail = controller.signal.aborted
          ? `Request timed out after ${this.#environment.timeoutMs}ms`
          : "Request failed";
        throw new KitaruApiError(method, path, null, detail, {
          cause: error,
        });
      }

      let value: unknown;
      try {
        value = await response.json();
      } catch (error) {
        if (error instanceof SyntaxError) {
          value = undefined;
        } else {
          if (attempt + 1 < attempts) {
            continue;
          }
          const detail = controller.signal.aborted
            ? `Request timed out after ${this.#environment.timeoutMs}ms`
            : "Request failed";
          throw new KitaruApiError(method, path, null, detail, {
            cause: error,
          });
        }
      } finally {
        clearTimeout(timeout);
      }
      if (!response.ok) {
        if (
          attempt + 1 < attempts &&
          RETRYABLE_UPSERT_STATUSES.has(response.status)
        ) {
          continue;
        }
        throw new KitaruApiError(
          method,
          path,
          response.status,
          safeErrorDetail(value, response.statusText || "Request failed"),
        );
      }
      return { status: response.status, value };
    }

    throw new Error("Unreachable request state");
  }
}
