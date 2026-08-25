import type { HttpMethod, ResponseValidator } from "../transport.js";
import type {
  AccountResponse,
  AgentResponse,
  AgentVersionResponse,
  AnnotationResponse,
  BlobResponse,
  CohortResponse,
  CohortVersionResponse,
  EvaluationResponse,
  EvaluatorResponse,
  EvaluatorVersionResponse,
  ExperimentResponse,
  ExperimentRunResponse,
  InvestigationResponse,
  InvestigationSessionResponse,
  JobResponse,
  Page,
  ServerInfoResponse,
  SessionWithNodesResponse,
  TaskResponse,
} from "../types.js";
import {
  invalidResponse,
  isRecord,
  requireResponseEnum as requireEnum,
  requireResponseString as requireString,
  requireResponseUuid as requireUuid,
} from "../validation.js";
import { validateNodes, validateSession } from "../validators.js";

const AUTH_SCHEMES = new Set(["none", "local", "control_plane"]);
const JOB_KINDS = new Set(["session_run", "import", "evaluation", "replay"]);
const JOB_STATUSES = new Set([
  "pending",
  "running",
  "completed",
  "failed",
  "canceled",
]);
const INVESTIGATION_STATUSES = new Set(["pending", "in_progress", "completed"]);
const INVESTIGATION_VERDICTS = new Set([
  "acceptable",
  "problematic",
  "uncertain",
]);
const EVALUATION_DATA_TYPES = new Set(["float", "bool", "str", "categorical"]);
const PLUGIN_SOURCE_TYPES = new Set(["script", "package"]);
const EXPERIMENT_RUN_STATUSES = new Set([
  "running",
  "canceling",
  "completed",
  "failed",
  "canceled",
]);
const TASK_KINDS = new Set(["agent", "evaluator", "importer"]);
const TASK_ON_FAILURE = new Set(["abort", "continue", "ignore"]);
const TASK_STATUSES = new Set([
  "pending",
  "claimed",
  "running",
  "completed",
  "failed",
  "timed_out",
  "canceled",
  "abandoned",
]);

function requireRecord(
  value: unknown,
  label: string,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is Record<string, unknown> {
  if (!isRecord(value)) {
    invalidResponse(method, path, status, `expected ${label}`);
  }
}

function requireNumber(
  value: Record<string, unknown>,
  property: string,
  method: HttpMethod,
  path: string,
  status: number,
): void {
  if (
    typeof value[property] !== "number" ||
    !Number.isFinite(value[property])
  ) {
    invalidResponse(method, path, status, `invalid ${property}`);
  }
}

export function validateAccount(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is AccountResponse {
  requireRecord(value, "an account object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireString(value, "name", method, path, status);
  if (
    typeof value.active !== "boolean" ||
    typeof value.is_admin !== "boolean" ||
    typeof value.is_service_account !== "boolean" ||
    !isRecord(value.metadata)
  ) {
    invalidResponse(method, path, status, "invalid account fields");
  }
}

export function validateInfo(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ServerInfoResponse {
  requireRecord(value, "a server info object", method, path, status);
  requireString(value, "version", method, path, status);
  requireEnum(value, "auth_scheme", AUTH_SCHEMES, method, path, status);
}

export function validateAgent(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is AgentResponse {
  requireRecord(value, "an agent object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireString(value, "name", method, path, status);
  requireNumber(value, "latest_version", method, path, status);
}

export function validateAgentVersion(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is AgentVersionResponse {
  requireRecord(value, "an agent version object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "agent_id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireNumber(value, "version", method, path, status);
  if (!isRecord(value.capabilities)) {
    invalidResponse(method, path, status, "invalid capabilities");
  }
}

export function validateBlob(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is BlobResponse {
  requireRecord(value, "a blob object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireString(value, "media_type", method, path, status);
  requireString(value, "sha256", method, path, status);
  requireNumber(value, "size", method, path, status);
}

export function validateJob(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is JobResponse {
  requireRecord(value, "a job object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireEnum(value, "kind", JOB_KINDS, method, path, status);
  requireEnum(value, "status", JOB_STATUSES, method, path, status);
}

export function validateInvestigation(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is InvestigationResponse {
  requireRecord(value, "an investigation object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "agent_id", method, path, status);
  requireString(value, "name", method, path, status);
  requireEnum(value, "status", INVESTIGATION_STATUSES, method, path, status);
  requireNumber(value, "completed_sessions", method, path, status);
  requireNumber(value, "total_sessions", method, path, status);
  if (!isRecord(value.metadata)) {
    invalidResponse(method, path, status, "invalid metadata");
  }
}

export function validateInvestigationSession(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is InvestigationSessionResponse {
  requireRecord(value, "an investigation session object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "investigation_id", method, path, status);
  requireUuid(value, "session_id", method, path, status);
  requireNumber(value, "position", method, path, status);
  if (!Array.isArray(value.questions)) {
    invalidResponse(method, path, status, "invalid questions");
  }
  for (const question of value.questions) {
    requireRecord(question, "an investigation question", method, path, status);
    requireString(question, "key", method, path, status);
    requireString(question, "question", method, path, status);
  }
  if (
    value.verdict !== null &&
    (typeof value.verdict !== "string" ||
      !INVESTIGATION_VERDICTS.has(value.verdict))
  ) {
    invalidResponse(method, path, status, "invalid verdict");
  }
}

export function validateAnnotation(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is AnnotationResponse {
  requireRecord(value, "an annotation object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "session_id", method, path, status);
  if (!Object.hasOwn(value, "value")) {
    invalidResponse(method, path, status, "missing value");
  }
}

export function validateEvaluator(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is EvaluatorResponse {
  requireRecord(value, "an evaluator object", method, path, status);
  requireUuid(value, "id", method, path, status);
  if (value.owner_id !== null) {
    requireUuid(value, "owner_id", method, path, status);
  }
  if (value.agent_id !== null) {
    requireUuid(value, "agent_id", method, path, status);
  }
  requireString(value, "name", method, path, status);
  requireNumber(value, "latest_version", method, path, status);
  if (!isRecord(value.metadata)) {
    invalidResponse(method, path, status, "invalid metadata");
  }
}

export function validateEvaluatorVersion(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is EvaluatorVersionResponse {
  requireRecord(value, "an evaluator version object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "evaluator_id", method, path, status);
  requireNumber(value, "version", method, path, status);
  requireRecord(value.source, "an evaluator source", method, path, status);
  if (
    typeof value.source.type !== "string" ||
    !PLUGIN_SOURCE_TYPES.has(value.source.type)
  ) {
    invalidResponse(method, path, status, "invalid source.type");
  }
  requireString(value.source, "entrypoint", method, path, status);
  if (value.source.type === "script") {
    requireUuid(value.source, "blob_id", method, path, status);
  } else {
    requireString(value.source, "requirement", method, path, status);
  }
}

export function validateEvaluation(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is EvaluationResponse {
  requireRecord(value, "an evaluation object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "session_id", method, path, status);
  requireString(value, "name", method, path, status);
  requireEnum(value, "data_type", EVALUATION_DATA_TYPES, method, path, status);
}

export function validateExperiment(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ExperimentResponse {
  requireRecord(value, "an experiment object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "agent_id", method, path, status);
  requireString(value, "name", method, path, status);
  if (!Array.isArray(value.evaluators) || !isRecord(value.tool_policy)) {
    invalidResponse(method, path, status, "invalid experiment configuration");
  }
}

export function validateExperimentRun(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is ExperimentRunResponse {
  requireRecord(value, "an experiment run object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "experiment_id", method, path, status);
  requireUuid(value, "cohort_version_id", method, path, status);
  requireUuid(value, "agent_version_id", method, path, status);
  requireNumber(value, "number", method, path, status);
  requireEnum(value, "status", EXPERIMENT_RUN_STATUSES, method, path, status);
  requireRecord(
    value.progress,
    "experiment run progress",
    method,
    path,
    status,
  );
  for (const field of [
    "pending",
    "evaluating",
    "completed",
    "failed",
    "canceled",
    "total",
  ]) {
    requireNumber(value.progress, field, method, path, status);
  }
  if (typeof value.evaluate_baselines !== "boolean") {
    invalidResponse(method, path, status, "invalid evaluate_baselines");
  }
}

export function validateTask(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is TaskResponse {
  requireRecord(value, "a task object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "job_id", method, path, status);
  requireEnum(value, "kind", TASK_KINDS, method, path, status);
  requireEnum(value, "status", TASK_STATUSES, method, path, status);
  requireEnum(value, "on_failure", TASK_ON_FAILURE, method, path, status);
  requireNumber(value, "attempt", method, path, status);
  if (!isRecord(value.labels) || !Object.hasOwn(value, "result")) {
    invalidResponse(method, path, status, "invalid task diagnostics");
  }
}

export function validateCohort(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is CohortResponse {
  requireRecord(value, "a cohort object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "agent_id", method, path, status);
  requireString(value, "name", method, path, status);
  requireNumber(value, "latest_version", method, path, status);
  if (!isRecord(value.metadata)) {
    invalidResponse(method, path, status, "invalid metadata");
  }
}

export function validateCohortVersion(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is CohortVersionResponse {
  requireRecord(value, "a cohort version object", method, path, status);
  requireUuid(value, "id", method, path, status);
  requireUuid(value, "owner_id", method, path, status);
  requireUuid(value, "cohort_id", method, path, status);
  requireNumber(value, "version", method, path, status);
  requireNumber(value, "session_count", method, path, status);
}

export function validateSessionWithNodes(
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
): asserts value is SessionWithNodesResponse {
  requireRecord(value, "a session with nodes object", method, path, status);
  validateSession(value.session, method, path, status);
  validateNodes(value.nodes, method, path, status);
}

export function createPageValidator<T>(
  validateItem: ResponseValidator<T>,
): ResponseValidator<Page<T>> {
  return (
    value: unknown,
    method: HttpMethod,
    path: string,
    status: number,
  ): asserts value is Page<T> => {
    if (
      !isRecord(value) ||
      !Array.isArray(value.items) ||
      !(value.next_cursor === null || typeof value.next_cursor === "string")
    ) {
      invalidResponse(method, path, status, "expected a page object");
    }
    for (const item of value.items) {
      validateItem(item, method, path, status);
    }
  };
}
