import type { SessionNodeCreateRequest } from "@zenml-io/kitaru";

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function tokenCount(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : undefined;
}

export function getModelTokens(
  value: unknown,
): SessionNodeCreateRequest["tokens"] {
  const usage = asRecord(value);
  const input = asRecord(usage.inputTokens);
  const output = asRecord(usage.outputTokens);
  const inputDetails = asRecord(usage.inputTokenDetails);
  const outputDetails = asRecord(usage.outputTokenDetails);
  const tokens = {
    input_tokens: tokenCount(usage.inputTokens) ?? tokenCount(input.total),
    output_tokens: tokenCount(usage.outputTokens) ?? tokenCount(output.total),
    cached_input_tokens:
      tokenCount(usage.cachedInputTokens) ??
      tokenCount(inputDetails.cacheReadTokens) ??
      tokenCount(input.cacheRead),
    reasoning_tokens:
      tokenCount(usage.reasoningTokens) ??
      tokenCount(outputDetails.reasoningTokens) ??
      tokenCount(output.reasoning),
  };
  return Object.values(tokens).some((count) => count !== undefined)
    ? tokens
    : null;
}
