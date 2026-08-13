import { toRecorderJson } from "../json.js";
import type { JsonValue, SessionNodeCreateRequest } from "../types.js";
import { providerFamily } from "./provider.js";
import type { AdapterRunState } from "./run-state.js";

type ToolLedgerEntry = NonNullable<ReturnType<AdapterRunState["getToolCall"]>>;

export interface NormalizedToolResult {
  error?: string;
  failed: boolean;
  output: JsonValue;
}

export interface NormalizedToolCall {
  callId: string;
  inputs: JsonValue;
  inputsLossy?: boolean;
  publicError?: string;
  result?: NormalizedToolResult;
  startedAt?: string;
  toolName: string;
}

export interface NormalizedModelStep {
  attributes: Record<string, JsonValue>;
  cost?: number | string | null;
  endedAt?: string;
  error?: string;
  externalId?: string;
  failed: boolean;
  inputs: JsonValue;
  model?: string;
  outputs: JsonValue;
  provider?: string;
  startedAt?: string;
  tokens?: SessionNodeCreateRequest["tokens"];
  tools: NormalizedToolCall[];
}

/**
 * Describe how a recorded tool call was decided and how faithfully it was kept.
 *
 * `inputs_bounded` says the recorded arguments are a bounded copy rather than
 * the arguments themselves, which is why the call is not eligible for a history
 * lookup: an operator reading the trace would otherwise see arguments that look
 * ordinary and wonder why no recorded result matched them. How a call was
 * decided is read from a different place on each path, so the caller supplies
 * those attributes and only the faithfulness rule is stated here.
 */
function toolCallAttributes(
  policyAttributes: Record<string, JsonValue>,
  inputsBounded: boolean,
): Record<string, JsonValue> {
  return inputsBounded
    ? { ...policyAttributes, inputs_bounded: true }
    : policyAttributes;
}

function policyAttributesFromLedger(
  ledgerEntry: ToolLedgerEntry | undefined,
): Record<string, JsonValue> {
  if (!ledgerEntry?.mocked || !ledgerEntry.policy) {
    return {};
  }
  return { mocked: true, policy: ledgerEntry.policy };
}

function toolNode(
  state: AdapterRunState,
  call: NormalizedToolCall,
  parentIndex: number,
  endedAt: string,
): SessionNodeCreateRequest {
  const ledgerEntry = state.getToolCall(call.callId);
  const failed =
    ledgerEntry?.outcome === "failed" ||
    call.result === undefined ||
    call.result.failed;
  return {
    ...state.allocateNode(),
    attributes: toolCallAttributes(
      policyAttributesFromLedger(ledgerEntry),
      call.inputsLossy === true || ledgerEntry?.inputsLossy === true,
    ),
    ended_at: endedAt,
    error: failed
      ? (ledgerEntry?.error?.message ??
        call.result?.error ??
        call.publicError ??
        "Tool did not produce a result")
      : null,
    external_id: call.callId,
    inputs: call.inputs,
    name: call.toolName,
    node_type: "tool_call",
    outputs: call.result?.output ?? ledgerEntry?.output ?? null,
    parent_index: parentIndex,
    started_at: call.startedAt ?? ledgerEntry?.startedAt,
    status: failed ? "failed" : "completed",
    tool_name: call.toolName,
  };
}

export async function recordNormalizedStep(
  state: AdapterRunState,
  step: NormalizedModelStep,
): Promise<void> {
  await state.enqueueStep(async () => {
    const endedAt = step.endedAt ?? new Date().toISOString();
    const startedAt = step.startedAt ?? state.takeStepStart(endedAt);
    const llmAllocation = state.allocateNode();
    const provider = step.provider;
    const llmNode: SessionNodeCreateRequest = {
      ...llmAllocation,
      attributes:
        provider === undefined
          ? step.attributes
          : { ...step.attributes, provider_id: provider },
      cost: step.cost ?? null,
      ended_at: endedAt,
      error: step.failed ? (step.error ?? "Model step failed") : null,
      external_id: step.externalId,
      inputs: step.inputs,
      model: step.model,
      model_params: state.effectiveModelSettings,
      name: "model_request",
      node_type: "llm_call",
      outputs: step.outputs,
      parent_index: state.rootIndex,
      model_provider:
        provider === undefined ? undefined : providerFamily(provider),
      requested_model: state.requestedModelId,
      started_at: startedAt,
      status: step.failed ? "failed" : "completed",
      tokens: step.tokens ?? null,
    };
    const matchedCallIds = step.tools.flatMap((call) =>
      state.getToolCall(call.callId) ? [call.callId] : [],
    );
    const nodes = [
      llmNode,
      ...step.tools.map((call) =>
        toolNode(state, call, llmAllocation.index, endedAt),
      ),
    ];

    await state.client.upsertSessionNodes(state.sessionId, { nodes });
    state.clearLedger(matchedCallIds);
  });
}

export async function flushFailedPolicyOutcomes(
  state: AdapterRunState,
): Promise<void> {
  const failedEntries = state.failedLedgerEntries();
  if (failedEntries.length === 0) {
    return;
  }
  const endedAt = new Date().toISOString();
  const nodes: SessionNodeCreateRequest[] = failedEntries.map((entry) => ({
    ...state.allocateNode(),
    attributes: toolCallAttributes(
      entry.policy === undefined
        ? {}
        : { mocked: entry.mocked, policy: entry.policy },
      entry.inputsLossy === true,
    ),
    ended_at: endedAt,
    error: entry.error?.message ?? "Tool policy failed",
    external_id: entry.callId,
    inputs: entry.inputs,
    name: entry.toolName,
    node_type: "tool_call",
    outputs: entry.output ?? null,
    parent_index: state.rootIndex,
    started_at: entry.startedAt,
    status: "failed",
    tool_name: entry.toolName,
  }));

  await state.client.upsertSessionNodes(state.sessionId, { nodes });
  state.clearLedger(failedEntries.map((entry) => entry.callId));
}

export function serializedSettings(
  settings: Record<string, unknown> | undefined,
): Record<string, JsonValue> | undefined {
  if (settings === undefined) {
    return undefined;
  }
  const serialized = toRecorderJson(settings);
  if (
    typeof serialized !== "object" ||
    serialized === null ||
    Array.isArray(serialized)
  ) {
    throw new TypeError("modelSettings must be a JSON object");
  }
  return serialized;
}
