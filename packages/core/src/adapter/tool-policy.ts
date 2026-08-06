import { computeToolCacheKey } from "../cache-key.js";
import { ToolPolicyError, ToolPolicyMissError } from "../errors.js";
import { recorderError, toRecorderJson } from "../json.js";
import type { JsonValue, ToolPolicy } from "../types.js";
import type { AdapterRunState } from "./run-state.js";

type ToolLedgerEntry = NonNullable<ReturnType<AdapterRunState["getToolCall"]>>;

export type ToolPolicyDecision =
  | { type: "execute" }
  | { output: unknown; type: "mocked_result" };

export interface ToolCallInput {
  callId: string;
  inputs: JsonValue;
  toolName: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepJsonEqual(left: JsonValue, right: JsonValue): boolean {
  if (left === right) {
    return true;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((item, index) =>
        deepJsonEqual(item, right[index] as JsonValue),
      )
    );
  }
  if (!isRecord(left) || !isRecord(right)) {
    return false;
  }
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key) =>
        Object.hasOwn(right, key) &&
        deepJsonEqual(left[key] as JsonValue, right[key] as JsonValue),
    )
  );
}

function staticCaseMatches(
  input: JsonValue,
  match: unknown,
  mode: "exact" | "subset",
): boolean {
  if (match == null) {
    return true;
  }
  const serializedMatch = toRecorderJson(match);
  if (mode === "exact") {
    return deepJsonEqual(input, serializedMatch);
  }
  if (!isRecord(input) || !isRecord(serializedMatch)) {
    return false;
  }
  return Object.entries(serializedMatch).every(
    ([key, value]) =>
      Object.hasOwn(input, key) &&
      deepJsonEqual(input[key] as JsonValue, value as JsonValue),
  );
}

export function selectToolPolicy(
  spec: NonNullable<AdapterRunState["spec"]>,
  toolName: string,
): ToolPolicy {
  const config = spec.tool_policy;
  if (config.tools && Object.hasOwn(config.tools, toolName)) {
    return config.tools[toolName] as ToolPolicy;
  }
  return config.default;
}

function policyMiss(
  entry: ToolLedgerEntry,
  policy: "history" | "static",
  onMiss: "error_result" | "fail" | "passthrough",
): ToolPolicyDecision {
  if (onMiss === "passthrough") {
    entry.policy = undefined;
    return { type: "execute" };
  }
  const message = `No ${policy} result for tool '${entry.toolName}'`;
  entry.policy = policy;
  if (onMiss === "error_result") {
    const output = { error: message };
    entry.mocked = true;
    entry.output = output;
    entry.error = { message, name: "ToolPolicyMiss" };
    entry.outcome = "failed";
    return { output, type: "mocked_result" };
  }
  const error = new ToolPolicyMissError(message);
  entry.error = { message, name: error.name };
  entry.outcome = "failed";
  stateFailure(entry, error);
  throw error;
}

function stateFailure(entry: ToolLedgerEntry, error: unknown): void {
  // The caller stores the same failure on its invocation state in the catch path.
  entry.error = recorderError(error);
  entry.outcome = "failed";
}

export async function decideToolCall(
  state: AdapterRunState,
  input: ToolCallInput,
): Promise<ToolPolicyDecision> {
  const entry: ToolLedgerEntry = {
    callId: input.callId,
    inputs: input.inputs,
    mocked: false,
    outcome: "pending",
    toolName: input.toolName,
  };
  state.setToolCall(entry);

  try {
    if (!state.spec) {
      throw new ToolPolicyError("Replay spec omitted its tool policy");
    }
    const policy = selectToolPolicy(state.spec, input.toolName);
    if (policy.type === "passthrough") {
      return { type: "execute" };
    }
    if (policy.type === "llm") {
      throw new ToolPolicyError(
        "Tool policy 'llm' is not supported by TypeScript adapters",
      );
    }
    if (policy.type === "static") {
      const matchingCase = policy.cases.find((candidate) =>
        staticCaseMatches(input.inputs, candidate.match, candidate.match_mode),
      );
      if (matchingCase) {
        entry.mocked = true;
        entry.outcome = "completed";
        entry.output = toRecorderJson(matchingCase.result);
        entry.policy = "static";
        return { output: matchingCase.result, type: "mocked_result" };
      }
      return policyMiss(entry, "static", policy.on_miss);
    }

    entry.policy = "history";
    const cacheKey = computeToolCacheKey(input.toolName, input.inputs);
    if (cacheKey === undefined) {
      return policyMiss(entry, "history", policy.on_miss);
    }
    if (!state.replayId) {
      throw new ToolPolicyError("History policy requires a replay ID");
    }
    const lookup = await state.client.lookupToolResult(state.replayId, {
      cache_key: cacheKey,
      tool_name: input.toolName,
    });
    if (lookup.found) {
      entry.mocked = true;
      entry.outcome = "completed";
      entry.output = toRecorderJson(lookup.result);
      return { output: lookup.result, type: "mocked_result" };
    }
    return policyMiss(entry, "history", policy.on_miss);
  } catch (error) {
    if (entry.outcome !== "failed") {
      stateFailure(entry, error);
    }
    state.storeFailure(error);
    throw error;
  }
}

function requiredEntry(
  state: AdapterRunState,
  callId: string,
): ToolLedgerEntry {
  const entry = state.getToolCall(callId);
  if (!entry) {
    throw new ToolPolicyError(`No replay decision for tool call '${callId}'`);
  }
  return entry;
}

export function completeToolCall(
  state: AdapterRunState,
  callId: string,
  output: unknown,
): void {
  const entry = requiredEntry(state, callId);
  if (entry.mocked) {
    return;
  }
  entry.output = toRecorderJson(output);
  entry.outcome = "completed";
}

export function failToolCall(
  state: AdapterRunState,
  callId: string,
  error: unknown,
): void {
  const entry = requiredEntry(state, callId);
  if (entry.mocked) {
    return;
  }
  entry.error = recorderError(error);
  entry.outcome = "failed";
}

export function isMockedToolCall(
  state: AdapterRunState,
  callId: string,
): boolean {
  return requiredEntry(state, callId).mocked;
}
