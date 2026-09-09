import type { Agent } from "@mastra/core/agent";
import type { MastraModelConfig } from "@mastra/core/llm";
import type { JsonValue, SessionNodeCreateRequest } from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  boundedRecorderJson,
  MAX_RECORDED_STRING_CHARS,
  projectRecordedMetadata,
  providerFamily,
  resolveCost,
} from "@zenml-io/kitaru/adapter";

import type { KitaruCostCalculator } from "./types.js";

export interface StructuredOutputModelOptions {
  modelConfig: MastraModelConfig;
  resolveModel: (config: MastraModelConfig) => ReturnType<Agent["getModel"]>;
  getState: () => Promise<AdapterRunState>;
  costCalculator?: KitaruCostCalculator;
  onAttemptFinish?: (error: Error | undefined) => void;
}

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

function getTokens(value: unknown): SessionNodeCreateRequest["tokens"] {
  const usage = asRecord(value);
  const input = asRecord(usage.inputTokens);
  const output = asRecord(usage.outputTokens);
  const tokens = {
    input_tokens: tokenCount(usage.inputTokens) ?? tokenCount(input.total),
    output_tokens: tokenCount(usage.outputTokens) ?? tokenCount(output.total),
    cached_input_tokens:
      tokenCount(usage.cachedInputTokens) ?? tokenCount(input.cacheRead),
    reasoning_tokens:
      tokenCount(usage.reasoningTokens) ?? tokenCount(output.reasoning),
  };
  return Object.values(tokens).some((count) => count !== undefined)
    ? tokens
    : null;
}

function getErrorMessage(error: unknown, fallback: string): string {
  const message = error instanceof Error ? error.message : error;
  return typeof message === "string" && message.length > 0
    ? message.slice(0, MAX_RECORDED_STRING_CHARS)
    : fallback;
}

function redactTransportMetadata(value: JsonValue): JsonValue {
  if (Array.isArray(value)) return value.map(redactTransportMetadata);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => {
      const normalized = key.toLowerCase().replace(/[-_]/g, "");
      return [
        key,
        normalized.includes("headers") || normalized.includes("apikey")
          ? "[redacted]"
          : redactTransportMetadata(item),
      ];
    }),
  );
}

// Select generation settings explicitly: transport headers, credentials, and
// abort signals must never become recorded model parameters.
function getModelParams(
  options: Record<string, unknown>,
): Record<string, JsonValue> {
  const params: Record<string, JsonValue> = {};
  for (const key of [
    "maxOutputTokens",
    "temperature",
    "topP",
    "topK",
    "presencePenalty",
    "frequencyPenalty",
    "stopSequences",
    "seed",
    "responseFormat",
    "reasoning",
  ]) {
    if (options[key] !== undefined) {
      params[key] = boundedRecorderJson(
        options[key],
        `structured output ${key}`,
      );
    }
  }
  return params;
}

/** Resolve and instrument the public model used for a structured-output call. */
export async function prepareStructuredOutputModel(
  options: StructuredOutputModelOptions,
): Promise<MastraModelConfig> {
  const model = await options.resolveModel(options.modelConfig);
  if (!["v2", "v3", "v4"].includes(model.specificationVersion)) {
    throw new TypeError(
      "Kitaru structuredOutput.model requires a v2, v3, or v4 model",
    );
  }
  const requestedModelId =
    typeof options.modelConfig === "string"
      ? options.modelConfig
      : typeof asRecord(options.modelConfig).id === "string"
        ? String(asRecord(options.modelConfig).id)
        : model.modelId;

  async function doStream(callOptions: unknown): Promise<unknown> {
    const state = await options.getState();
    const startedAt = new Date().toISOString();
    const parameters = asRecord(callOptions);
    let text = "";
    let usage: unknown;
    let finishReason: unknown;
    let finished = false;
    let providerMetadata: unknown;
    let responseId: string | undefined;
    let servedModelId = model.modelId;
    let streamError: unknown;
    let recording: Promise<void> | undefined;
    let cancelled = false;
    let cancellationReason: unknown;

    function record(failed: boolean, error?: unknown): Promise<void> {
      recording ??= writeRecord(failed, error).then(() => {
        options.onAttemptFinish?.(
          failed
            ? new Error(
                getErrorMessage(error, "Structured output model failed"),
              )
            : undefined,
        );
      });
      return recording;
    }

    async function writeRecord(
      failed: boolean,
      error?: unknown,
    ): Promise<void> {
      const endedAt = new Date().toISOString();
      try {
        await state.enqueueStep(async () => {
          const tokens = getTokens(usage);
          const cost = await resolveCost(options.costCalculator, {
            model: servedModelId,
            provider: model.provider,
            requestedModelId,
            tokens,
          });
          const node: SessionNodeCreateRequest = {
            ...state.allocateNode(),
            name: "structured_output",
            node_type: "llm_call",
            parent_index: state.rootIndex,
            started_at: startedAt,
            ended_at: endedAt,
            status: failed ? "failed" : "completed",
            error: failed
              ? getErrorMessage(error, "Structured output model failed")
              : null,
            requested_model: requestedModelId,
            model: servedModelId,
            model_provider: providerFamily(model.provider),
            model_params: getModelParams(parameters),
            external_id: responseId,
            inputs: boundedRecorderJson(
              parameters.prompt,
              "structured output prompt",
            ),
            outputs: boundedRecorderJson(
              { text, finish_reason: finishReason },
              "structured output result",
            ),
            attributes: {
              structured_output: true,
              provider_id: model.provider,
              cost: cost.attribute,
              provider_metadata: redactTransportMetadata(
                projectRecordedMetadata(providerMetadata),
              ),
            },
            tokens,
            cost: cost.cost,
          };
          await state.client.upsertSessionNodes(state.sessionId, {
            nodes: [node],
          });
        });
      } catch (recordingError) {
        // Keep provider errors intact; the parent run checks this failure even
        // when Mastra consumes a stream error internally.
        state.storeFailure(recordingError);
      }
    }

    let result: { stream: ReadableStream<unknown> };
    try {
      result = await Reflect.apply(model.doStream, model, [callOptions]);
    } catch (error) {
      await record(true, error);
      throw error;
    }
    const reader = result.stream.getReader();
    const stream = new ReadableStream<unknown>({
      async pull(controller) {
        try {
          const next = await reader.read();
          if (next.done) {
            await record(
              cancelled ||
                !finished ||
                streamError !== undefined ||
                finishReason === "error",
              cancelled
                ? cancellationReason
                : (streamError ??
                    (!finished
                      ? "Structured output stream ended without a finish event"
                      : undefined)),
            );
            if (!cancelled) controller.close();
            reader.releaseLock();
            return;
          }
          const chunk = asRecord(next.value);
          if (chunk.type === "text-delta") {
            const delta =
              typeof chunk.delta === "string" ? chunk.delta : chunk.textDelta;
            if (typeof delta === "string")
              text = (text + delta).slice(0, MAX_RECORDED_STRING_CHARS + 1);
          } else if (chunk.type === "response-metadata") {
            if (typeof chunk.id === "string") responseId = chunk.id;
            if (typeof chunk.modelId === "string")
              servedModelId = chunk.modelId;
          } else if (chunk.type === "finish") {
            finished = true;
            usage = chunk.usage;
            finishReason =
              typeof chunk.finishReason === "string"
                ? chunk.finishReason
                : asRecord(chunk.finishReason).unified;
            providerMetadata = chunk.providerMetadata;
          } else if (chunk.type === "error") {
            streamError =
              chunk.error ?? new Error("Structured output model failed");
          }
          if (!cancelled) controller.enqueue(next.value);
        } catch (error) {
          await record(true, error);
          if (!cancelled) controller.error(error);
          reader.releaseLock();
        }
      },
      async cancel(reason) {
        cancelled = true;
        cancellationReason = reason ?? "Structured output stream cancelled";
        try {
          await reader.cancel(reason);
        } finally {
          await record(true, reason ?? "Structured output stream cancelled");
          reader.releaseLock();
        }
      },
    });
    // Mastra attaches per-call transport metadata with non-enumerable symbols.
    // Preserve those descriptors without depending on their private names.
    return Object.create(Object.getPrototypeOf(result), {
      ...Object.getOwnPropertyDescriptors(result),
      stream: {
        value: stream,
        enumerable: true,
        configurable: true,
        writable: true,
      },
    });
  }

  // Preserve Mastra's public wrapper identity so its child agent does not
  // adapt this model again. Getters and methods need the original receiver
  // because those wrappers use native private fields internally.
  return new Proxy(model, {
    get(target, key) {
      if (key === "doStream") return doStream;
      const value = Reflect.get(target, key, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
