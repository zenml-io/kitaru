import type { InputProcessor } from "@mastra/core/processors";
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
} from "@mastra/core/request-context";
import type { JsonValue } from "@zenml-io/kitaru";
import { recordedToolPayloadConversion } from "@zenml-io/kitaru/adapter";
import { canCaptureMemoryContext } from "./memory-context-support.js";
import type { RuntimeGenerateOptions } from "./types.js";

export const CONTEXT_KEY = "mastra_conversation_context";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function hasMemoryOptions(options: RuntimeGenerateOptions): boolean {
  return (
    options.memory !== undefined ||
    options.threadId !== undefined ||
    options.resourceId !== undefined ||
    options.requestContext?.get(MASTRA_THREAD_ID_KEY) !== undefined ||
    options.requestContext?.get(MASTRA_RESOURCE_ID_KEY) !== undefined ||
    options.requestContext?.get("MastraMemory") !== undefined
  );
}

export function unsupportedContext(reason?: string): Error {
  return new Error(
    `Unsupported Mastra replay: the original invocation's complete conversation context is unavailable. ${reason ?? "Record a history-only invocation again with this adapter, or supply its complete recorded message array without live memory selectors."} Newer thread history cannot substitute for the original context.`,
  );
}

export function restoreConversationContext(
  input: unknown,
): unknown[] | undefined {
  if (!isRecord(input) || !Object.hasOwn(input, CONTEXT_KEY)) {
    return undefined;
  }
  const context = input[CONTEXT_KEY];
  if (
    !isRecord(context) ||
    context.version !== 1 ||
    context.source !== "recalled" ||
    context.complete !== true ||
    !Array.isArray(context.messages) ||
    context.messages.length === 0 ||
    context.messages.some(
      (message) =>
        !isRecord(message) ||
        !["system", "user", "assistant", "tool"].includes(
          String(message.role),
        ) ||
        (typeof message.content !== "string" &&
          !Array.isArray(message.content)),
    )
  ) {
    throw unsupportedContext(
      isRecord(context) && typeof context.reason === "string"
        ? context.reason
        : undefined,
    );
  }
  return context.messages;
}

export function createContextInput(
  supplied: JsonValue,
  messages?: unknown[],
  unsupportedReason?: string,
): JsonValue {
  const converted = recordedToolPayloadConversion(
    messages ?? [],
    "Mastra conversation context",
  );
  return {
    [CONTEXT_KEY]: {
      version: 1,
      source: "recalled",
      complete: messages !== undefined && !converted.lossy,
      messages: converted.value,
      ...(messages === undefined || converted.lossy
        ? {
            reason: converted.lossy
              ? "The snapshot lost content during safe serialization; provide a complete serializable conversation without credentials."
              : (unsupportedReason ??
                "No complete snapshot was captured; record a history-only invocation that reaches the model input hook."),
          }
        : {}),
    },
    supplied_messages: supplied,
  };
}

export function createContextProcessor(
  capture: (messages: unknown[] | undefined) => Promise<void>,
): InputProcessor {
  let captured = false;
  return {
    id: "kitaru-record-conversation-context",
    async processInputStep({ messageList, requestContext }) {
      if (!captured) {
        captured = true;
        await capture(
          canCaptureMemoryContext(requestContext)
            ? messageList.get.all.aiV5.prompt()
            : undefined,
        );
      }
    },
  };
}
