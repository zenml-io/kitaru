import { historyCacheKey } from "../cache-key.js";
import { ToolPolicyError, ToolPolicyMissError } from "../errors.js";
import { recorderError, toRecorderJson } from "../json.js";
import type { JsonValue, ToolLookupRequest, ToolPolicy } from "../types.js";
import { isRecord } from "../validation.js";
import { recordedToolPayloadJson } from "./recorded-json.js";
import type { AdapterRunState } from "./run-state.js";

type ToolLedgerEntry = NonNullable<ReturnType<AdapterRunState["getToolCall"]>>;

export type ToolPolicyDecision =
  | { type: "execute" }
  | { output: unknown; type: "mocked_error" }
  | { output: unknown; type: "mocked_result" };

export interface ToolCallInput {
  callId: string;
  inputs: JsonValue;
  // True when converting the arguments for recording dropped or altered part
  // of them. Set it from the converter rather than guessing: a value that lost
  // information no longer identifies the call it came from.
  inputsLossy?: boolean;
  // The untouched arguments supplied by the framework. Static policies match
  // these instead of the bounded/redacted ledger value.
  originalInputs?: unknown;
  toolName: string;
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

function staticMatchingInput(input: ToolCallInput): JsonValue {
  if (Object.hasOwn(input, "originalInputs")) {
    return toRecorderJson(input.originalInputs);
  }
  if (input.inputsLossy === true) {
    throw new ToolPolicyError(
      `Tool '${input.toolName}' arguments could not be matched against static ` +
        "cases because only a lossy recorded value was available",
    );
  }
  return input.inputs;
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

export type SupportedToolPolicy = Exclude<ToolPolicy, { type: "llm" }>;

/**
 * Select a tool's policy and reject the ones no TypeScript adapter can run.
 */
export function assertSupportedToolPolicy(
  spec: NonNullable<AdapterRunState["spec"]>,
  toolName: string,
): SupportedToolPolicy {
  const policy = selectToolPolicy(spec, toolName);
  if (policy.type === "llm") {
    throw new ToolPolicyError(
      "Tool policy 'llm' is not supported by TypeScript adapters",
    );
  }
  return policy;
}

/**
 * Reject a tool the adapter cannot intercept before the replay starts.
 *
 * A tool the framework runs elsewhere, or one with no local execute function,
 * never reaches the Kitaru hooks, so a replay would let it fire for real.
 */
export function assertInterceptableTool(
  toolName: string,
  interceptable: boolean,
): void {
  if (!interceptable) {
    throw new ToolPolicyError(
      `Replay requires a local execute function for tool '${toolName}'`,
    );
  }
}

function policyMiss(
  entry: ToolLedgerEntry,
  policy: "history" | "static",
  onMiss: "error_result" | "fail" | "passthrough",
  message = `No ${policy} result for tool '${entry.toolName}'`,
): ToolPolicyDecision {
  if (onMiss === "passthrough") {
    entry.policy = undefined;
    return { type: "execute" };
  }
  entry.policy = policy;
  if (onMiss === "error_result") {
    const output = { error: message };
    entry.mocked = true;
    entry.output = output;
    entry.error = { message, name: "ToolPolicyMiss" };
    entry.outcome = "failed";
    return { output, type: "mocked_error" };
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
  // Every tool call passes through here, so this is the one place that can
  // stop a later tool from firing its side effect for real once a policy has
  // already failed, whatever the framework does with the rejected call.
  if (state.failure !== undefined) {
    throw state.failure;
  }
  const entry: ToolLedgerEntry = {
    callId: input.callId,
    inputs: input.inputs,
    inputsLossy: input.inputsLossy,
    mocked: false,
    outcome: "pending",
    startedAt: new Date().toISOString(),
    toolName: input.toolName,
  };
  state.setToolCall(entry);

  try {
    if (!state.spec) {
      throw new ToolPolicyError("Replay spec omitted its tool policy");
    }
    const policy = assertSupportedToolPolicy(state.spec, input.toolName);
    if (policy.type === "passthrough") {
      return { type: "execute" };
    }
    if (policy.type === "static") {
      const matchingInput = staticMatchingInput(input);
      const matchingCase = policy.cases.find((candidate) =>
        staticCaseMatches(matchingInput, candidate.match, candidate.match_mode),
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
    const cacheKey = historyCacheKey(
      input.toolName,
      input.inputs,
      input.inputsLossy === true,
    );
    if (cacheKey === undefined) {
      // A call with no key of its own never gets to look: the lookup would
      // confidently return another call's recorded result.
      return policyMiss(
        entry,
        "history",
        policy.on_miss,
        input.inputsLossy === true
          ? `Tool '${input.toolName}' arguments could not be recorded ` +
              "losslessly, so they could not be matched against recorded history"
          : undefined,
      );
    }
    if (!state.replayId) {
      throw new ToolPolicyError("History policy requires a replay ID");
    }
    const request: ToolLookupRequest = {
      cache_key: cacheKey,
      tool_name: input.toolName,
    };
    // Baseline lookups walk repeated identical calls through the recorded
    // occurrences in order; other scopes have no stable per-run ordering, so
    // they take the server's default (newest match) instead.
    const occurrence =
      policy.scope === "baseline"
        ? state.getHistoryOccurrence(cacheKey)
        : undefined;
    if (occurrence !== undefined) {
      request.occurrence = occurrence;
    }
    const lookup = await state.client.lookupToolResult(state.replayId, request);
    // Any hit consumes its recorded occurrence, even one the ambiguity guard
    // below rejects, matching the Python adapters; a miss keeps the counter
    // still so a later identical call can retry the same recorded position.
    if (lookup.found && occurrence !== undefined) {
      state.advanceHistoryOccurrence(cacheKey, occurrence);
    }
    if (lookup.found && lookup.result === null) {
      throw new ToolPolicyError(
        `History lookup for tool '${input.toolName}' cannot distinguish a ` +
          "failed recording from a null result; refusing to execute or mock it",
      );
    }
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
  // A passthrough tool during a replay has already fired its side effect by
  // the time its result is recorded, so an oversized or circular result is
  // bounded rather than thrown: crashing here would strand a sent email.
  entry.output = recordedToolPayloadJson(
    output,
    `tool '${entry.toolName}' output`,
  );
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
