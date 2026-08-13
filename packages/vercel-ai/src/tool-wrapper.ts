import {
  computeToolCacheKey,
  type JsonValue,
  type ReplaySpec,
  ToolPolicyError,
} from "@zenml-io/kitaru";
import {
  type AdapterRunState,
  assertInterceptableTool,
  assertSupportedToolPolicy,
  completeToolCall,
  decideToolCall,
  failToolCall,
  recordedPayloadJson,
  selectToolPolicy,
} from "@zenml-io/kitaru/adapter";
import {
  asSchema,
  type LanguageModelCallEndEvent,
  type Tool,
  type ToolExecutionOptions,
  type ToolSet,
} from "ai";

import type { ExecutionTickets } from "./tickets.js";

type ExecutableTool = Tool & {
  execute: (input: never, options: ToolExecutionOptions<never>) => unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isExecutable(tool: Tool | undefined): tool is ExecutableTool {
  return tool !== undefined && typeof tool.execute === "function";
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof value[Symbol.asyncIterator] === "function"
  );
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    "then" in value &&
    typeof value.then === "function"
  );
}

export function assertSupportedReplayTools(options: {
  generationOptions: Record<string, unknown>;
  spec: ReplaySpec;
  tools: ToolSet | undefined;
}): void {
  if (options.generationOptions.toolApproval !== undefined) {
    throw new ToolPolicyError("Replay does not support toolApproval");
  }
  if (options.generationOptions.experimental_sandbox !== undefined) {
    throw new ToolPolicyError("Replay does not support sandboxed tools");
  }
  for (const [toolName, tool] of Object.entries(options.tools ?? {})) {
    if (tool.needsApproval !== undefined && tool.needsApproval !== false) {
      throw new ToolPolicyError(
        `Replay does not support approval-gated tool '${toolName}'`,
      );
    }
    if (tool.type === "provider") {
      throw new ToolPolicyError(
        `Replay cannot intercept provider tool '${toolName}'`,
      );
    }
    if (tool.type === "dynamic") {
      throw new ToolPolicyError(
        `Replay cannot intercept dynamic tool '${toolName}'`,
      );
    }
    assertInterceptableTool(toolName, isExecutable(tool));
    assertSupportedToolPolicy(options.spec, toolName);
  }
}

async function validateMockedOutput(
  tool: Tool,
  output: unknown,
  toolName: string,
): Promise<JsonValue> {
  const bounded = recordedPayloadJson(output, `mocked '${toolName}' output`);
  if (tool.outputSchema === undefined) {
    return bounded;
  }
  const schema = asSchema(tool.outputSchema);
  if (schema.validate === undefined) {
    return bounded;
  }
  const validation = await schema.validate(bounded);
  if (!validation.success) {
    throw new ToolPolicyError(
      `Mocked output for tool '${toolName}' failed its output schema`,
      { cause: validation.error },
    );
  }
  return recordedPayloadJson(
    validation.value,
    `validated mocked '${toolName}' output`,
  );
}

function setBaselineLedger(
  state: AdapterRunState,
  callId: string,
  toolName: string,
  input: JsonValue,
): void {
  state.setToolCall({
    callId,
    inputs: input,
    mocked: false,
    outcome: "pending",
    toolName,
  });
}

function storeAdapterFailure(state: AdapterRunState, error: unknown): void {
  state.storeFailure(error);
}

function completeRecordedTool(
  state: AdapterRunState,
  callId: string,
  output: unknown,
): void {
  try {
    completeToolCall(state, callId, output);
  } catch (error) {
    storeAdapterFailure(state, error);
    throw error;
  }
}

function failApplicationTool(
  state: AdapterRunState,
  callId: string,
  error: unknown,
): void {
  const entry = state.getToolCall(callId);
  if (entry && entry.outcome === "pending") {
    failToolCall(state, callId, error);
  }
}

async function* recordBaselineIterable(options: {
  callId: string;
  output: AsyncIterable<unknown>;
  state: AdapterRunState;
}): AsyncIterable<unknown> {
  let lastOutput: unknown;
  try {
    for await (const item of options.output) {
      lastOutput = item;
      yield item;
    }
  } catch (error) {
    failApplicationTool(options.state, options.callId, error);
    throw error;
  }
  completeRecordedTool(options.state, options.callId, lastOutput);
}

function executeBaseline(options: {
  callId: string;
  execute: ExecutableTool["execute"];
  executionOptions: ToolExecutionOptions<never>;
  input: never;
  serializedInput: JsonValue;
  state: AdapterRunState;
  toolName: string;
}): unknown {
  setBaselineLedger(
    options.state,
    options.callId,
    options.toolName,
    options.serializedInput,
  );
  let output: unknown;
  try {
    output = options.execute(options.input, options.executionOptions);
  } catch (error) {
    failApplicationTool(options.state, options.callId, error);
    throw error;
  }
  if (isAsyncIterable(output)) {
    return recordBaselineIterable({
      callId: options.callId,
      output,
      state: options.state,
    });
  }
  if (isPromiseLike(output)) {
    return Promise.resolve(output).then(
      (resolved) => {
        completeRecordedTool(options.state, options.callId, resolved);
        return resolved;
      },
      (error: unknown) => {
        failApplicationTool(options.state, options.callId, error);
        throw error;
      },
    );
  }
  completeRecordedTool(options.state, options.callId, output);
  return output;
}

/**
 * Warn once when a replay repeats a tool call with identical arguments.
 *
 * Kitaru looks recorded results up by (tool name, arguments), so every repeat
 * of the same call resolves to the last recorded result for that pair. The
 * replayed trajectory then diverges from the baseline, and only the run itself
 * can tell the operator that happened.
 */
function warnOnRepeatedCall(options: {
  cacheKey: string | undefined;
  seen: Set<string>;
  toolName: string;
  warned: Set<string>;
}): void {
  // The cache key is a fixed-size digest of the same (tool name, arguments)
  // pair the lookup uses, so a long generation does not retain a copy of every
  // serialized tool input just to spot the repeats.
  const key = options.cacheKey;
  if (key === undefined) {
    return;
  }
  if (!options.seen.has(key)) {
    options.seen.add(key);
    return;
  }
  if (options.warned.has(key)) {
    return;
  }
  options.warned.add(key);
  console.warn(
    `Kitaru replay: tool '${options.toolName}' was called again with identical ` +
      "arguments. Recorded results are keyed by tool name and arguments, so " +
      "every repeat resolves to the last recorded result for that pair.",
  );
}

export function wrapTools<TOOLS extends ToolSet>(options: {
  state: AdapterRunState;
  tickets: ExecutionTickets;
  tools: TOOLS | undefined;
}): TOOLS | undefined {
  if (!options.tools) {
    return undefined;
  }
  const wrapped: Record<string, Tool> = Object.create(null);
  const warnedRepeats = new Set<string>();
  const replayedCalls = new Set<string>();
  for (const [toolName, tool] of Object.entries(options.tools)) {
    if (!isExecutable(tool)) {
      wrapped[toolName] = tool;
      continue;
    }
    const originalExecute = tool.execute;
    wrapped[toolName] = {
      ...tool,
      execute: (
        input: never,
        executionOptions: ToolExecutionOptions<never>,
      ) => {
        const callId = executionOptions.toolCallId;
        const serializedInput = recordedPayloadJson(
          input,
          `tool '${toolName}' input`,
        );
        const spec = options.state.spec;
        if (!spec) {
          return executeBaseline({
            callId,
            execute: originalExecute,
            executionOptions,
            input,
            serializedInput,
            state: options.state,
            toolName,
          });
        }
        const executeReplay = async () => {
          // Under passthrough a repeated call runs live and resolves to its own
          // result, so the warning would be both wrong and pure cost.
          if (selectToolPolicy(spec, toolName).type === "history") {
            warnOnRepeatedCall({
              cacheKey: computeToolCacheKey(toolName, serializedInput),
              seen: replayedCalls,
              toolName,
              warned: warnedRepeats,
            });
          }
          const decision = await decideToolCall(options.state, {
            callId,
            inputs: serializedInput,
            toolName,
          });
          if (decision.type === "mocked_result") {
            try {
              return await validateMockedOutput(
                tool,
                decision.output,
                toolName,
              );
            } catch (error) {
              storeAdapterFailure(options.state, error);
              throw error;
            }
          }
          let output: unknown;
          try {
            output = originalExecute(input, executionOptions);
            if (isAsyncIterable(output)) {
              throw new ToolPolicyError(
                `Replay does not support async-iterable tool '${toolName}'`,
              );
            }
            output = await output;
          } catch (error) {
            if (error instanceof ToolPolicyError) {
              storeAdapterFailure(options.state, error);
            } else {
              failApplicationTool(options.state, callId, error);
            }
            throw error;
          }
          completeRecordedTool(options.state, callId, output);
          return output;
        };
        return options.tickets
          .run(
            callId,
            executeReplay,
            executionOptions.abortSignal,
            () => options.state.failure !== undefined,
            () => options.state.failure,
          )
          .catch((error: unknown) => {
            const entry = options.state.getToolCall(callId);
            const ordinaryApplicationError =
              entry?.outcome === "failed" &&
              options.state.failure === undefined;
            if (!ordinaryApplicationError) {
              storeAdapterFailure(
                options.state,
                options.state.failure ?? error,
              );
            }
            throw error;
          });
      },
    } as Tool;
  }
  return wrapped as TOOLS;
}

export function registerReplayTickets<TOOLS extends ToolSet>(options: {
  event: LanguageModelCallEndEvent<TOOLS>;
  state: AdapterRunState;
  tickets: ExecutionTickets;
  tools: TOOLS | undefined;
}): void {
  try {
    const callIds = options.event.content.flatMap((part) => {
      if (
        part.type !== "tool-call" ||
        part.invalid ||
        part.providerExecuted ||
        !isExecutable(options.tools?.[part.toolName])
      ) {
        return [];
      }
      return [part.toolCallId];
    });
    options.tickets.register(callIds);
  } catch (error) {
    options.state.storeFailure(error);
    throw error;
  }
}

export function hasApprovalResponse(messages: unknown): boolean {
  if (!Array.isArray(messages)) {
    return false;
  }
  return messages.some(
    (message) =>
      isRecord(message) &&
      Array.isArray(message.content) &&
      message.content.some(
        (part) =>
          isRecord(part) &&
          (part.type === "tool-approval-response" ||
            part.type === "tool-approval-request"),
      ),
  );
}
