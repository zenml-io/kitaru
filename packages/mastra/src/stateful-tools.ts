import { TripWire } from "@mastra/core/agent";
import type { InputProcessor } from "@mastra/core/processors";
import type { Memory } from "@mastra/memory";
import {
  type AdapterRunState,
  assertInterceptableTool,
  assertSupportedToolPolicy,
  mastraReplayToolConversion,
  type RecordingLimits,
} from "@zenml-io/kitaru/adapter";
import { assertStableToolName } from "./replay-guards.js";
import { createToolHooks } from "./tool-policies.js";
import type { KitaruAgentOptions } from "./types.js";

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** Brand only tools returned by this invocation's native Memory, before native conversion. */
export function bindMemoryToolIdentity(memory: Memory) {
  const tokens = new Set<string>();
  const copies = new WeakMap<object, object>();
  const bound = new Proxy(memory, {
    get(target, key) {
      if (key === "listTools")
        return (...args: Parameters<Memory["listTools"]>) =>
          Object.fromEntries(
            Object.entries(target.listTools(...args)).map(([name, tool]) => {
              let copy = copies.get(tool);
              if (!copy) {
                const id = `kitaru-memory-${globalThis.crypto.randomUUID()}`;
                tokens.add(id);
                copy = { ...tool, id };
                copies.set(tool, copy);
              }
              return [name, copy];
            }),
          );
      const value = Reflect.get(target, key, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return { memory: bound, tokens };
}

const TRIPWIRE_METHODS = new Set<PropertyKey>([
  "processInput",
  "processInputStep",
]);

/**
 * Wrap an input processor so a tripwire it raises also reaches `onTripwire`.
 *
 * A processor tripwire, for example an `abort()` or a failed blocking
 * observation, ends Mastra's run without calling `onFinish`, `onError` or
 * `onAbort`. The tripwire still propagates to Mastra unchanged, once the
 * promise `onTripwire` returns has settled.
 */
export function reportProcessorTripwires<T>(
  processor: T,
  onTripwire: (reason: string) => Promise<void> | void,
): T {
  if (!record(processor)) return processor;
  return new Proxy(processor, {
    get(target, key) {
      const value = Reflect.get(target, key, target);
      if (!TRIPWIRE_METHODS.has(key) || typeof value !== "function")
        return typeof value === "function" ? value.bind(target) : value;
      return async (...args: unknown[]) => {
        try {
          return await Reflect.apply(value, target, args);
        } catch (error) {
          if (error instanceof TripWire) {
            try {
              // Mastra ends the stream once the tripwire propagates, so a
              // replay closes its session first; a caller may exit as soon
              // as it has read the stream.
              await onTripwire(error.message);
            } catch {
              // Recording diagnostics never replace Mastra's tripwire.
            }
          }
          throw error;
        }
      };
    },
  });
}

/** Report a tripwire raised by one of the Memory's own input processors. */
export function reportMemoryProcessorTripwires(
  memory: Memory,
  onTripwire: (reason: string) => Promise<void> | void,
): Memory {
  const wrapped = new WeakMap<object, unknown>();
  const watch = (processor: unknown): unknown => {
    if (!record(processor)) return processor;
    let copy = wrapped.get(processor);
    if (!copy) {
      copy = reportProcessorTripwires(processor, onTripwire);
      wrapped.set(processor, copy);
    }
    return copy;
  };
  return new Proxy(memory, {
    get(target, key) {
      const value = Reflect.get(target, key, target);
      if (key !== "getInputProcessors" || typeof value !== "function")
        return typeof value === "function" ? value.bind(target) : value;
      return async (...args: unknown[]) => {
        const processors: unknown = await Reflect.apply(value, target, args);
        return Array.isArray(processors) ? processors.map(watch) : processors;
      };
    },
  });
}

/** Enforce policies on the final executable inventory, including processor-added tools. */
export function createStatefulToolProcessors(options: {
  tokens: ReadonlySet<string>;
  getState(): AdapterRunState;
  abort(reason: unknown): void;
  adapter: KitaruAgentOptions;
  /** Per-value bounds the application chose; the replay budget applies otherwise. */
  recordingLimits?: RecordingLimits;
  sanitizeEvidence?: <T>(value: T) => T;
}) {
  const trusted = new WeakSet<(...args: never[]) => unknown>();
  const wrappersByName = new Map<
    string,
    WeakMap<
      (...args: never[]) => unknown,
      (...args: unknown[]) => Promise<unknown>
    >
  >();
  const first: InputProcessor = {
    id: "kitaru-memory-tool-identity",
    processInputStep({ tools }) {
      for (const tool of Object.values(tools ?? {})) {
        if (
          record(tool) &&
          typeof tool.id === "string" &&
          options.tokens.has(tool.id) &&
          typeof tool.execute === "function"
        )
          trusted.add(tool.execute as (...args: never[]) => unknown);
      }
    },
  };
  const last: InputProcessor = {
    id: "kitaru-final-tool-policy",
    processInputStep({ tools }) {
      const output: Record<string, unknown> = {};
      for (const [name, tool] of Object.entries(tools ?? {})) {
        if (
          !record(tool) ||
          typeof tool.execute !== "function" ||
          (tool.requireApproval !== undefined &&
            tool.requireApproval !== false) ||
          tool.hasSuspendSchema === true ||
          tool.type === "provider-defined"
        )
          throw new Error(
            `Unsupported replay tool '${name}': an ordinary interceptable executor is required.`,
          );
        assertStableToolName(name);
        assertInterceptableTool(name, true);
        const state = options.getState();
        if (state.spec) assertSupportedToolPolicy(state.spec, name);
        const wrappers = wrappersByName.get(name) ?? new WeakMap();
        wrappersByName.set(name, wrappers);
        const execute = tool.execute as (...args: never[]) => unknown;
        let wrapper = wrappers.get(execute);
        if (!wrapper) {
          const isMemory = trusted.has(execute);
          const hooks = createToolHooks({
            state,
            abortReplay: options.abort,
            trustedMemoryTool: isMemory,
            configuredBeforeToolCall: options.adapter.configuredBeforeToolCall,
            configuredAfterToolCall: options.adapter.configuredAfterToolCall,
            limits: options.recordingLimits,
            convertPayload: mastraReplayToolConversion,
            sanitizeEvidence: options.sanitizeEvidence,
          });
          wrapper = async (input: unknown, context: unknown) => {
            const event = { toolName: name, input, context, metadata: {} };
            const before = await hooks.beforeToolCall?.(event);
            if (before?.proceed === false) return before.output;
            let result: unknown;
            try {
              result = await Reflect.apply(execute, tool, [input, context]);
            } catch (error) {
              await hooks.afterToolCall?.({
                ...event,
                output: undefined,
                error,
              });
              throw error;
            }
            await hooks.afterToolCall?.({ ...event, output: result });
            return result;
          };
          wrappers.set(execute, wrapper);
          wrappers.set(wrapper, wrapper);
        }
        output[name] = { ...tool, execute: wrapper };
      }
      return { tools: output };
    },
  };
  return { first, last };
}
