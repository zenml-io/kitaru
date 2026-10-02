import type {
  ToolAfterHookContext,
  ToolBeforeHookResult,
  ToolHookContext,
  ToolHooks,
} from "@mastra/core/tools";
import { ToolPolicyError } from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  boundedRecorderConversion,
  completeToolCall,
  decideToolCall,
  failToolCall,
  isMockedToolCall,
  type RecordedConversion,
  type RecordingLimits,
} from "@zenml-io/kitaru/adapter";

import type {
  ConfiguredAfterToolCall,
  ConfiguredBeforeToolCall,
} from "./types.js";

interface ToolHookOptions {
  trustedMemoryTool?: boolean;
  abortReplay?: (reason: unknown) => void;
  callerHooks?: ToolHooks;
  configuredAfterToolCall?: ConfiguredAfterToolCall;
  configuredBeforeToolCall?: ConfiguredBeforeToolCall;
  limits?: RecordingLimits;
  /** Convert tool arguments and results for recording. */
  convertPayload?: PayloadConversion;
  sanitizeEvidence?: <T>(value: T) => T;
  state: AdapterRunState;
}

type PayloadConversion = (
  value: unknown,
  path: string,
  limits?: RecordingLimits,
) => RecordedConversion;

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
  limits?: RecordingLimits,
  sanitizeEvidence?: <T>(value: T) => T,
  convertPayload?: PayloadConversion,
): Promise<undefined | ToolBeforeHookResult<unknown>> {
  try {
    const configuredResult = await configuredHook?.(hookContext);
    if (isSkippedResult(configuredResult)) {
      completeToolCall(
        state,
        callId,
        sanitizeEvidence?.(configuredResult.output) ?? configuredResult.output,
        limits,
        convertPayload,
      );
      return configuredResult;
    }
    const callerResult = await callerHook?.(hookContext);
    if (isSkippedResult(callerResult)) {
      completeToolCall(
        state,
        callId,
        sanitizeEvidence?.(callerResult.output) ?? callerResult.output,
        limits,
        convertPayload,
      );
      return callerResult;
    }
    return undefined;
  } catch (error) {
    failToolCall(state, callId, error);
    throw error;
  }
}

export function createToolHooks(options: ToolHookOptions): ToolHooks {
  const {
    abortReplay,
    callerHooks,
    configuredAfterToolCall,
    configuredBeforeToolCall,
    convertPayload = boundedRecorderConversion,
    limits,
    sanitizeEvidence,
    state,
  } = options;

  return {
    beforeToolCall: async (hookContext) => {
      try {
        // Mastra keeps the loop alive after a rejected hook, so every later tool
        // has to refuse to run rather than fire its side effect for real.
        if (state.failure !== undefined) {
          throw state.failure;
        }
        const callId = toolCallId(hookContext.context);
        const converted = convertPayload(
          sanitizeEvidence?.(hookContext.input) ?? hookContext.input,
          `tool '${hookContext.toolName}' input`,
          limits,
        );
        if (state.spec && !options.trustedMemoryTool) {
          const decision = await decideToolCall(state, {
            callId,
            inputs: converted.value,
            inputsLossy: converted.lossy,
            originalInputs:
              sanitizeEvidence?.(hookContext.input) ?? hookContext.input,
            recordOutput: (value) =>
              convertPayload(
                value,
                `tool '${hookContext.toolName}' output`,
                limits,
              ).value,
            toolName: hookContext.toolName,
          });
          if (decision.type !== "execute") {
            return { output: decision.output, proceed: false };
          }
        } else {
          state.setToolCall({
            callId,
            inputs: converted.value,
            inputsLossy: converted.lossy,
            mocked: false,
            outcome: "pending",
            startedAt: new Date().toISOString(),
            toolName: hookContext.toolName,
          });
        }
        return invokePassthroughBeforeHooks(
          state,
          callId,
          hookContext,
          configuredBeforeToolCall,
          callerHooks?.beforeToolCall,
          limits,
          sanitizeEvidence,
          convertPayload,
        );
      } catch (error) {
        abortReplay?.(error);
        throw error;
      }
    },
    afterToolCall: async (hookContext: ToolAfterHookContext) => {
      const callId = toolCallId(hookContext.context);
      if (isMockedToolCall(state, callId)) {
        return;
      }

      if (hookContext.error !== undefined) {
        failToolCall(state, callId, hookContext.error);
      } else {
        completeToolCall(
          state,
          callId,
          sanitizeEvidence?.(hookContext.output) ?? hookContext.output,
          limits,
          convertPayload,
        );
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
