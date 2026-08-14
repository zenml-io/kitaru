import {
  type JsonValue,
  recorderError,
  type SessionNodeCreateRequest,
} from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  boundedRecordedText,
  boundRecordedSize,
  type NormalizedToolCall,
  projectRecordedMetadata,
  type RecordedConversion,
  recordedPayloadConversion,
  recordedPayloadJson,
  recordNormalizedStep,
  resolveCost,
} from "@zenml-io/kitaru/adapter";
import type {
  ContentPart,
  LanguageModelCallStartEvent,
  StepResult,
  ToolSet,
} from "ai";

import type { KitaruCostCalculator } from "./types.js";

function usageTokens(
  usage: StepResult<ToolSet>["usage"],
): SessionNodeCreateRequest["tokens"] {
  const fields = {
    cached_input_tokens: usage.inputTokenDetails.cacheReadTokens,
    input_tokens: usage.inputTokens,
    output_tokens: usage.outputTokens,
    reasoning_tokens: usage.outputTokenDetails.reasoningTokens,
  };
  return Object.values(fields).some((value) => value !== undefined)
    ? fields
    : null;
}

function resultPart(
  content: readonly ContentPart<ToolSet>[],
  callId: string,
): ContentPart<ToolSet> | undefined {
  return content.find(
    (part) =>
      (part.type === "tool-result" || part.type === "tool-error") &&
      part.toolCallId === callId,
  );
}

// The tool wrapper converts every tool input it intercepts and stores the
// result on the run's ledger, so the recorder reads that back rather than
// walking the same payload a second time. Converting twice also computes the
// lossy verdict twice, and the trace attribute and the history-lookup guard
// have to tell the same story about the call. A tool the wrapper never saw
// (provider-executed, or one with no local execute function) has no ledger
// entry and is converted here.
function toolInputConversion(
  state: AdapterRunState,
  call: { input: unknown; toolCallId: string; toolName: string },
): RecordedConversion {
  const entry = state.getToolCall(call.toolCallId);
  return entry === undefined
    ? recordedPayloadConversion(call.input, `tool '${call.toolName}' input`)
    : { lossy: entry.inputsLossy === true, value: entry.inputs };
}

function normalizedTools(
  state: AdapterRunState,
  step: StepResult<ToolSet>,
): NormalizedToolCall[] {
  return step.toolCalls.map((call) => {
    const part = resultPart(step.content, call.toolCallId);
    const failed = part?.type === "tool-error";
    const inputs = toolInputConversion(state, call);
    return {
      callId: call.toolCallId,
      inputs: inputs.value,
      inputsLossy: inputs.lossy,
      publicError: failed ? "Tool execution failed" : undefined,
      result:
        part === undefined
          ? undefined
          : {
              error: failed ? "Tool execution failed" : undefined,
              failed,
              output:
                part.type === "tool-result"
                  ? recordedPayloadJson(
                      part.output,
                      `tool '${call.toolName}' output`,
                    )
                  : null,
            },
      toolName: call.toolName,
    };
  });
}

// The tool nodes and the model node record the same arguments and results, so
// the model node reuses what the tool nodes already converted. Converting them
// twice costs a second deep clone of every payload on the generation path.
function stepOutputs(
  step: StepResult<ToolSet>,
  tools: readonly NormalizedToolCall[],
): JsonValue {
  const converted = new Map(tools.map((call) => [call.callId, call]));
  return boundRecordedSize(
    {
      finish_reason: step.finishReason,
      reasoning_text: boundedRecordedText(step.reasoningText),
      text: boundedRecordedText(step.text),
      tool_calls: step.toolCalls.map((call) => ({
        input:
          converted.get(call.toolCallId)?.inputs ??
          recordedPayloadJson(call.input, `tool '${call.toolName}' input`),
        tool_call_id: call.toolCallId,
        tool_name: call.toolName,
      })),
      tool_results: step.toolResults.map((result) => {
        const recorded = converted.get(result.toolCallId)?.result;
        return {
          output:
            recorded && !recorded.failed
              ? recorded.output
              : recordedPayloadJson(
                  result.output,
                  `tool '${result.toolName}' output`,
                ),
          tool_call_id: result.toolCallId,
          tool_name: result.toolName,
        };
      }),
    },
    "model step output",
  );
}

function servedModelId(step: StepResult<ToolSet>): string {
  return step.response.modelId || step.model.modelId;
}

export async function recordVercelStep(
  state: AdapterRunState,
  step: StepResult<ToolSet>,
  costCalculator?: KitaruCostCalculator,
): Promise<void> {
  const tokens = usageTokens(step.usage);
  const cost = await resolveCost(costCalculator, {
    model: servedModelId(step),
    provider: step.model.provider,
    requestedModelId: state.requestedModelId,
    tokens,
  });
  const tools = normalizedTools(state, step);
  await recordNormalizedStep(state, {
    attributes: {
      cost: cost.attribute,
      finish_reason: step.finishReason,
      provider_metadata: projectRecordedMetadata(step.providerMetadata),
    },
    cost: cost.cost,
    externalId: step.response.id,
    failed: step.finishReason === "error",
    inputs: null,
    model: servedModelId(step),
    outputs: stepOutputs(step, tools),
    provider: step.model.provider,
    tokens,
    tools,
  });
}

export async function recordFailedVercelModelCall(
  state: AdapterRunState,
  call: Pick<LanguageModelCallStartEvent, "callId" | "modelId" | "provider"> & {
    startedAt: string;
  },
  error: unknown,
): Promise<void> {
  await recordNormalizedStep(state, {
    attributes: {},
    error: recorderError(error).message,
    externalId: call.callId,
    failed: true,
    inputs: null,
    model: call.modelId,
    outputs: null,
    provider: call.provider,
    startedAt: call.startedAt,
    tools: [],
  });
}
