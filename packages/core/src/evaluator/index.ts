import { Console } from "node:console";

import type { components } from "../generated/openapi.js";
import type {
  JsonValue,
  SessionDetailResponse,
  SessionNodeResponse,
} from "../types.js";

export type EvaluationResult = components["schemas"]["EvaluationResult"];
export type EvaluatorParams = Record<string, JsonValue>;

/** Full recorded session, including every node supplied by the evaluation task. */
export interface SessionView {
  session: SessionDetailResponse;
  nodes: SessionNodeResponse[];
}

export type Evaluator = (
  session: SessionView,
  params: EvaluatorParams,
) => EvaluationResult[] | Promise<EvaluationResult[]>;

export interface EvaluatorRequest {
  schema_version: 1;
  session: SessionView;
  params: EvaluatorParams;
}

export interface EvaluatorResponse {
  schema_version: 1;
  results: EvaluationResult[];
}

const RESULT_FIELDS = new Set([
  "name",
  "score",
  "value",
  "explanation",
  "passed",
  "min_score",
  "max_score",
  "target_score",
]);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Reject invalid or duplicate rows before any result is emitted. */
export function validateEvaluationResults(value: unknown): EvaluationResult[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new TypeError("Evaluator must return a nonempty array of results");
  }
  const names = new Set<string>();
  for (const result of value) {
    if (
      !isObject(result) ||
      Object.keys(result).some((key) => !RESULT_FIELDS.has(key))
    ) {
      throw new TypeError("Evaluator returned an invalid result object");
    }
    if (
      typeof result.name !== "string" ||
      result.name.length > 255 ||
      /^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$/.exec(result.name)?.[0] !==
        result.name
    ) {
      throw new TypeError(
        "Evaluation name must be 1-255 letters, digits, '.', '_' or '-', starting and ending with a letter or digit",
      );
    }
    if (names.has(result.name))
      throw new TypeError(`Duplicate evaluation name: ${result.name}`);
    names.add(result.name);
    if (result.score == null && result.value == null) {
      throw new TypeError(
        `Evaluation '${result.name}' must set score or value`,
      );
    }
    if (
      result.score != null &&
      typeof result.score !== "boolean" &&
      (typeof result.score !== "number" || !Number.isFinite(result.score))
    ) {
      throw new TypeError(
        `Evaluation '${result.name}' score must be finite or boolean`,
      );
    }
    for (const field of ["value", "explanation"] as const) {
      if (result[field] != null && typeof result[field] !== "string") {
        throw new TypeError(
          `Evaluation '${result.name}' ${field} must be a string`,
        );
      }
    }
    if (result.passed != null && typeof result.passed !== "boolean") {
      throw new TypeError(`Evaluation '${result.name}' passed must be boolean`);
    }
    for (const field of ["min_score", "max_score", "target_score"] as const) {
      if (
        result[field] != null &&
        (typeof result[field] !== "number" ||
          !Number.isFinite(result[field]) ||
          typeof result.score !== "number" ||
          result.value != null)
      ) {
        throw new TypeError(
          `Evaluation '${result.name}' ${field} requires a finite numeric score without a value`,
        );
      }
    }
  }
  return value as EvaluationResult[];
}

/** Execute one versioned request without writing partial results. */
export async function evaluateRequest(
  request: unknown,
  evaluator: Evaluator,
): Promise<EvaluatorResponse> {
  if (
    !isObject(request) ||
    request.schema_version !== 1 ||
    !isObject(request.session) ||
    !isObject(request.session.session) ||
    !Array.isArray(request.session.nodes) ||
    !request.session.nodes.every(isObject) ||
    !isObject(request.params)
  ) {
    throw new TypeError(
      "Expected evaluator protocol version 1 with session, nodes, and params",
    );
  }
  // The Python task validates the full API payload before sending this envelope.
  const input = request as unknown as EvaluatorRequest;
  return {
    schema_version: 1,
    results: validateEvaluationResults(
      await evaluator(input.session, input.params),
    ),
  };
}

/** Read one stdin request and write one JSON response. Await this at module scope. */
export async function runEvaluator(evaluator: Evaluator): Promise<void> {
  process.stdin.setEncoding("utf8");
  let input = "";
  for await (const chunk of process.stdin) input += chunk;
  // This dedicated process must redirect even diagnostics deferred by scorers.
  globalThis.console = new Console({
    stdout: process.stderr,
    stderr: process.stderr,
  });
  const response = await evaluateRequest(JSON.parse(input), evaluator);
  process.stdout.write(`${JSON.stringify(response)}\n`);
}
