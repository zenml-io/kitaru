import type { LLMStepResult } from "@mastra/core/stream";
import {
  type JsonValue,
  type SessionNodeCreateRequest,
  toRecorderJson,
} from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  type NormalizedToolCall,
  recordNormalizedStep,
} from "@zenml-io/kitaru/adapter";

import type { PublicModelIdentity } from "./types.js";

export type RecordedStep = LLMStepResult<unknown> & {
  error?: unknown;
  model?: PublicModelIdentity;
  runId?: string;
  text?: string;
};

interface ToolCallPayload {
  args: unknown;
  toolCallId: string;
  toolName: string;
}

interface ToolResultPayload extends ToolCallPayload {
  isError: boolean;
  result: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function payload(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) && isRecord(value.payload) ? value.payload : undefined;
}

function toolCallPayload(value: unknown): ToolCallPayload | undefined {
  const candidate = payload(value);
  if (
    !candidate ||
    typeof candidate.toolCallId !== "string" ||
    typeof candidate.toolName !== "string"
  ) {
    return undefined;
  }
  return {
    args: candidate.args,
    toolCallId: candidate.toolCallId,
    toolName: candidate.toolName,
  };
}

function toolResultPayload(value: unknown): ToolResultPayload | undefined {
  const candidate = toolCallPayload(value);
  const raw = payload(value);
  if (!candidate || !raw || !Object.hasOwn(raw, "result")) {
    return undefined;
  }
  return {
    ...candidate,
    isError: raw.isError === true,
    result: raw.result,
  };
}

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error) {
    return error.message || error.name;
  }
  if (isRecord(error) && typeof error.message === "string") {
    return error.message;
  }
  if (isRecord(error) && typeof error.value === "string") {
    return error.value;
  }
  if (typeof error === "string" && error.length > 0) {
    return error;
  }
  return fallback;
}

function usageTokens(usage: unknown): SessionNodeCreateRequest["tokens"] {
  if (!isRecord(usage)) {
    return null;
  }
  const inputDetails = isRecord(usage.inputTokenDetails)
    ? usage.inputTokenDetails
    : undefined;
  const outputDetails = isRecord(usage.outputTokenDetails)
    ? usage.outputTokenDetails
    : undefined;
  const fields = {
    cached_input_tokens:
      typeof usage.cachedInputTokens === "number"
        ? usage.cachedInputTokens
        : typeof inputDetails?.cacheReadTokens === "number"
          ? inputDetails.cacheReadTokens
          : undefined,
    input_tokens:
      typeof usage.inputTokens === "number" ? usage.inputTokens : undefined,
    output_tokens:
      typeof usage.outputTokens === "number" ? usage.outputTokens : undefined,
    reasoning_tokens:
      typeof usage.reasoningTokens === "number"
        ? usage.reasoningTokens
        : typeof outputDetails?.reasoningTokens === "number"
          ? outputDetails.reasoningTokens
          : undefined,
  };
  return Object.values(fields).some((value) => value !== undefined)
    ? fields
    : null;
}

function stepOutputs(step: RecordedStep, calls: ToolCallPayload[]): JsonValue {
  return toRecorderJson({
    content: step.content,
    finish_reason: step.finishReason,
    response_messages: step.response?.messages,
    text: step.text,
    tool_calls: calls,
    warnings: step.warnings,
  });
}

function toolErrorFromContent(
  content: unknown,
  callId: string,
): string | undefined {
  if (!Array.isArray(content)) {
    return undefined;
  }
  const resultPart = content.find(
    (part) =>
      isRecord(part) &&
      part.type === "tool-result" &&
      part.toolCallId === callId,
  );
  if (!isRecord(resultPart) || !Object.hasOwn(resultPart, "output")) {
    return undefined;
  }
  return errorMessage(resultPart.output, "Tool failed");
}

export async function recordStep(
  state: AdapterRunState,
  step: RecordedStep,
): Promise<void> {
  const calls = step.toolCalls.flatMap((item) => {
    const call = toolCallPayload(item);
    return call ? [call] : [];
  });
  const results = new Map(
    step.toolResults.flatMap((item) => {
      const result = toolResultPayload(item);
      return result ? [[result.toolCallId, result] as const] : [];
    }),
  );
  const tools: NormalizedToolCall[] = calls.map((call) => {
    const result = results.get(call.toolCallId);
    return {
      callId: call.toolCallId,
      inputs: toRecorderJson(call.args),
      publicError: toolErrorFromContent(step.content, call.toolCallId),
      result: result
        ? {
            error: result.isError
              ? errorMessage(result.result, "Tool failed")
              : undefined,
            failed: result.isError,
            output: toRecorderJson(result.result),
          }
        : undefined,
      toolName: call.toolName,
    };
  });
  const failed = step.finishReason === "error";

  await recordNormalizedStep(state, {
    attributes: isRecord(step.providerMetadata)
      ? { provider_metadata: toRecorderJson(step.providerMetadata) }
      : {},
    cost: null,
    error: failed ? errorMessage(step.error, "Model step failed") : undefined,
    externalId: step.response?.id,
    failed,
    inputs: toRecorderJson(step.request?.body),
    model: step.response?.modelId ?? step.model?.modelId,
    outputs: stepOutputs(step, calls),
    provider: step.model?.provider,
    tokens: usageTokens(step.usage),
    tools,
  });
}
