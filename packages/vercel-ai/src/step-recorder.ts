import type { JsonValue, SessionNodeCreateRequest } from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  type NormalizedToolCall,
  recordNormalizedStep,
} from "@zenml-io/kitaru/adapter";
import type { ContentPart, StepResult, ToolSet } from "ai";

import { boundedRecorderJson, projectRecordedMetadata } from "./options.js";

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

function normalizedTools(step: StepResult<ToolSet>): NormalizedToolCall[] {
  return step.toolCalls.map((call) => {
    const part = resultPart(step.content, call.toolCallId);
    const failed = part?.type === "tool-error";
    return {
      callId: call.toolCallId,
      inputs: boundedRecorderJson(call.input, `tool '${call.toolName}' input`),
      publicError: failed ? "Tool execution failed" : undefined,
      result:
        part === undefined
          ? undefined
          : {
              error: failed ? "Tool execution failed" : undefined,
              failed,
              output:
                part.type === "tool-result"
                  ? boundedRecorderJson(
                      part.output,
                      `tool '${call.toolName}' output`,
                    )
                  : null,
            },
      toolName: call.toolName,
    };
  });
}

function safeOutputs(step: StepResult<ToolSet>): JsonValue {
  return boundedRecorderJson(
    {
      finish_reason: step.finishReason,
      reasoning_text: step.reasoningText,
      text: step.text,
      tool_calls: step.toolCalls.map((call) => ({
        input: boundedRecorderJson(call.input, `tool '${call.toolName}' input`),
        tool_call_id: call.toolCallId,
        tool_name: call.toolName,
      })),
      tool_results: step.toolResults.map((result) => ({
        output: boundedRecorderJson(
          result.output,
          `tool '${result.toolName}' output`,
        ),
        tool_call_id: result.toolCallId,
        tool_name: result.toolName,
      })),
    },
    "model step output",
  );
}

export async function recordVercelStep(
  state: AdapterRunState,
  step: StepResult<ToolSet>,
): Promise<void> {
  await recordNormalizedStep(state, {
    attributes: {
      finish_reason: step.finishReason,
      provider_metadata: projectRecordedMetadata(step.providerMetadata),
    },
    cost: null,
    externalId: step.response.id,
    failed: step.finishReason === "error",
    inputs: null,
    model: step.model.modelId,
    outputs: safeOutputs(step),
    provider: step.model.provider,
    tokens: usageTokens(step.usage),
    tools: normalizedTools(step),
  });
}
