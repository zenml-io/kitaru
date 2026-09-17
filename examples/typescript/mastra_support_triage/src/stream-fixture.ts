import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";

import { lookupOrder } from "./tools.js";

export const STREAM_MODEL_ID = "support-stream-fixture";
export const STREAM_TEXT = "Order ord-1001 is delayed.";

function chunkStream(chunks: unknown[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

function createSuccessfulModel(): MastraLanguageModelV2Mock {
  let call = 0;
  return new MastraLanguageModelV2Mock({
    doStream: async () => {
      call += 1;
      return {
        stream: chunkStream(
          call === 1
            ? [
                { type: "stream-start", warnings: [] },
                {
                  id: "support-tool-response",
                  modelId: STREAM_MODEL_ID,
                  type: "response-metadata",
                },
                {
                  input: '{"orderId":"ord-1001"}',
                  toolCallId: "support-order-call",
                  toolName: "lookupOrder",
                  type: "tool-call",
                },
                {
                  finishReason: "tool-calls",
                  type: "finish",
                  usage: { inputTokens: 5, outputTokens: 2, totalTokens: 7 },
                },
              ]
            : [
                { type: "stream-start", warnings: [] },
                {
                  id: "support-text-response",
                  modelId: STREAM_MODEL_ID,
                  type: "response-metadata",
                },
                { id: "support-text", type: "text-start" },
                {
                  delta: "Order ord-1001 ",
                  id: "support-text",
                  type: "text-delta",
                },
                {
                  delta: "is delayed.",
                  id: "support-text",
                  type: "text-delta",
                },
                { id: "support-text", type: "text-end" },
                {
                  finishReason: "stop",
                  type: "finish",
                  usage: { inputTokens: 6, outputTokens: 4, totalTokens: 10 },
                },
              ],
        ) as never,
      };
    },
    modelId: STREAM_MODEL_ID,
    provider: "support-fixture",
  });
}

function createAbortableModel(): MastraLanguageModelV2Mock {
  return new MastraLanguageModelV2Mock({
    doStream: async (options) => {
      let emitted = false;
      return {
        stream: new ReadableStream({
          async pull(controller) {
            if (!emitted) {
              emitted = true;
              controller.enqueue({ id: "abort-text", type: "text-start" });
              controller.enqueue({
                delta: "partial",
                id: "abort-text",
                type: "text-delta",
              });
              return;
            }
            if (!options.abortSignal?.aborted) {
              await new Promise<void>((resolve) =>
                options.abortSignal?.addEventListener(
                  "abort",
                  () => resolve(),
                  {
                    once: true,
                  },
                ),
              );
            }
            controller.error(
              options.abortSignal?.reason ?? new Error("stream aborted"),
            );
          },
        }) as never,
      };
    },
    modelId: STREAM_MODEL_ID,
    provider: "support-fixture",
  });
}

export function createStreamingSupportAgent(abort = false): Agent {
  return new Agent({
    id: "kitaru-mastra-streaming-support-triage",
    instructions: "Look up the order, then summarize its delivery status.",
    model: abort ? createAbortableModel() : createSuccessfulModel(),
    name: "Kitaru Mastra streaming support triage",
    tools: { lookupOrder },
  });
}
