import type {
  ToolAfterHookContext,
  ToolBeforeHookResult,
  ToolHookContext,
  ToolHooks,
} from "@mastra/core/tools";
import { ToolPolicyError, toRecorderJson } from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  completeToolCall,
  decideToolCall,
  failToolCall,
  isMockedToolCall,
} from "@zenml-io/kitaru/adapter";

import type {
  ConfiguredAfterToolCall,
  ConfiguredBeforeToolCall,
} from "./types.js";

interface ReplayHookOptions {
  callerHooks?: ToolHooks;
  configuredAfterToolCall?: ConfiguredAfterToolCall;
  configuredBeforeToolCall?: ConfiguredBeforeToolCall;
  state: AdapterRunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toolCallId(context: unknown): string {
  if (isRecord(context) && typeof context.toolCallId === "string") {
    return context.toolCallId;
  }
  if (
    isRecord(context) &&
    isRecord(context.agent) &&
    typeof context.agent.toolCallId === "string"
  ) {
    return context.agent.toolCallId;
  }
  throw new ToolPolicyError(
    "Mastra tool hook context omitted the public tool-call ID",
  );
}

function isSkippedResult(
  value: unknown,
): value is ToolBeforeHookResult<unknown> {
  return (
    isRecord(value) && value.proceed === false && Object.hasOwn(value, "output")
  );
}

async function invokePassthroughBeforeHooks(
  state: AdapterRunState,
  callId: string,
  hookContext: ToolHookContext,
  configuredHook?: ConfiguredBeforeToolCall,
  callerHook?: ToolHooks["beforeToolCall"],
): Promise<undefined | ToolBeforeHookResult<unknown>> {
  try {
    const configuredResult = await configuredHook?.(hookContext);
    if (isSkippedResult(configuredResult)) {
      completeToolCall(state, callId, configuredResult.output);
      return configuredResult;
    }
    const callerResult = await callerHook?.(hookContext);
    if (isSkippedResult(callerResult)) {
      completeToolCall(state, callId, callerResult.output);
      return callerResult;
    }
    return undefined;
  } catch (error) {
    failToolCall(state, callId, error);
    throw error;
  }
}

export function createReplayToolHooks(options: ReplayHookOptions): ToolHooks {
  const {
    callerHooks,
    configuredAfterToolCall,
    configuredBeforeToolCall,
    state,
  } = options;

  return {
    beforeToolCall: async (hookContext) => {
      const callId = toolCallId(hookContext.context);
      const decision = await decideToolCall(state, {
        callId,
        inputs: toRecorderJson(hookContext.input),
        toolName: hookContext.toolName,
      });
      if (decision.type === "mocked_result") {
        return { output: decision.output, proceed: false };
      }
      return invokePassthroughBeforeHooks(
        state,
        callId,
        hookContext,
        configuredBeforeToolCall,
        callerHooks?.beforeToolCall,
      );
    },
    afterToolCall: async (hookContext: ToolAfterHookContext) => {
      const callId = toolCallId(hookContext.context);
      if (isMockedToolCall(state, callId)) {
        return;
      }

      if (hookContext.error !== undefined) {
        failToolCall(state, callId, hookContext.error);
      } else {
        completeToolCall(state, callId, hookContext.output);
      }

      try {
        await configuredAfterToolCall?.(hookContext);
        await callerHooks?.afterToolCall?.(hookContext);
      } catch (error) {
        failToolCall(state, callId, error);
        throw error;
      }
    },
  };
}
