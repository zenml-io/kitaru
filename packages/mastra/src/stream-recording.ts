import { createRequire } from "node:module";
import type { JsonValue, KitaruClient } from "@zenml-io/kitaru";
import {
  type RunRecorder,
  recordedToolPayloadJson,
  runResultSummary,
  serializedSettings,
} from "@zenml-io/kitaru/adapter";

import {
  createContextInput,
  createContextProcessor,
  hasMemoryOptions,
} from "./conversation-context.js";
import { type RecordedStep, recordStep } from "./step-recorder.js";
import { createToolHooks } from "./tool-policies.js";
import type {
  KitaruAgentOptions,
  RuntimeStreamOptions,
  StreamRecordingErrorStage,
} from "./types.js";

type StreamAgent = {
  generate: (...args: never[]) => unknown;
  stream?: (messages: unknown, options?: RuntimeStreamOptions) => unknown;
  getDefaultOptions?: (options?: { requestContext?: unknown }) => unknown;
  getToolsForExecution?: (options: Record<string, unknown>) => unknown;
  listConfiguredInputProcessors?: (requestContext?: unknown) => unknown;
};

interface StreamRecordingOptions {
  adapterVersion: string;
  agent: StreamAgent;
  callerMessages: unknown;
  callerOptions: RuntimeStreamOptions;
  client: KitaruClient;
  options: KitaruAgentOptions;
  replayInput: JsonValue;
  requestedModelId: string;
  sessionName?: string;
  startedAt: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getMastraVersion(): string {
  let metadata: unknown;
  try {
    metadata = createRequire(import.meta.url)("@mastra/core/package.json");
  } catch (error) {
    throw new TypeError(
      "KitaruAgent.stream() requires a readable stable @mastra/core 1.67.x package version",
      { cause: error },
    );
  }
  if (!isRecord(metadata) || typeof metadata.version !== "string") {
    throw new TypeError("Unable to read the installed @mastra/core version");
  }
  return metadata.version;
}

export function isSupportedMastraStreamVersion(version: string): boolean {
  return /^1\.67\.\d+$/.test(version);
}

export function assertStreamSupported(
  agent: StreamAgent,
): asserts agent is StreamAgent & {
  stream: NonNullable<StreamAgent["stream"]>;
} {
  const version = getMastraVersion();
  if (!isSupportedMastraStreamVersion(version)) {
    throw new Error(
      `KitaruAgent.stream() requires a stable @mastra/core 1.67.x installation; found '${version}'`,
    );
  }
  if (typeof agent.stream !== "function") {
    throw new TypeError("The wrapped agent does not provide a stream() method");
  }
}

function usesSecondaryStructuredModel(options: RuntimeStreamOptions): boolean {
  return (
    isRecord(options.structuredOutput) &&
    options.structuredOutput.model !== undefined &&
    options.structuredOutput.model !== null
  );
}

function assertDisabled(name: string, value: unknown): void {
  if (value !== undefined && value !== false && value !== null) {
    throw new TypeError(`KitaruAgent.stream() does not support ${name}`);
  }
}

async function assertSupportedOptions(
  agent: StreamAgent,
  options: RuntimeStreamOptions,
): Promise<void> {
  assertDisabled("requireToolApproval", options.requireToolApproval);
  assertDisabled("autoResumeSuspendedTools", options.autoResumeSuspendedTools);
  assertDisabled("untilIdle", options.untilIdle);
  assertDisabled("backgroundTaskPolicy", options.backgroundTaskPolicy);
  assertDisabled("resumeContext", options.resumeContext);
  assertDisabled("_skipBgTaskWait", options._skipBgTaskWait);
  if (usesSecondaryStructuredModel(options)) {
    throw new TypeError(
      "KitaruAgent.stream() does not support structuredOutput.model; schema-only structured output is supported",
    );
  }
  if (typeof agent.getToolsForExecution !== "function") return;
  const tools = await agent.getToolsForExecution({
    autoResumeSuspendedTools: options.autoResumeSuspendedTools,
    clientTools: options.clientTools,
    delegation: options.delegation,
    hooks: options.hooks,
    memoryConfig: options.memory,
    methodType: "stream",
    outputWriter: options.outputWriter,
    requestContext: options.requestContext,
    resourceId: options.resourceId,
    runId: options.runId,
    threadId: options.threadId,
    toolsets: options.toolsets,
  });
  if (!isRecord(tools)) return;
  for (const [name, tool] of Object.entries(tools)) {
    if (
      isRecord(tool) &&
      tool.requireApproval !== undefined &&
      tool.requireApproval !== false
    ) {
      throw new TypeError(
        `KitaruAgent.stream() does not support approval for tool '${name}'`,
      );
    }
    if (isRecord(tool) && tool.hasSuspendSchema === true) {
      throw new TypeError(
        `KitaruAgent.stream() does not support suspension for tool '${name}'`,
      );
    }
  }
}

function getTripwireReason(value: unknown): string | undefined {
  if (!isRecord(value) || !isRecord(value.tripwire)) return undefined;
  return typeof value.tripwire.reason === "string" && value.tripwire.reason
    ? value.tripwire.reason
    : "Mastra processor tripwire triggered";
}

class StreamLifecycle {
  #cleanupPromise?: Promise<void>;
  #completionStarted = false;
  #failureReason: unknown;
  #failureRequested = false;
  #finalizerPromise?: Promise<void>;
  #notified = false;
  #recordingError?: {
    error: unknown;
    stage: StreamRecordingErrorStage;
  };
  #stepTail: Promise<void> = Promise.resolve();

  constructor(
    readonly recorder: RunRecorder,
    readonly options: KitaruAgentOptions,
  ) {}

  async record(step: RecordedStep): Promise<void> {
    if (this.#recordingError !== undefined) return;
    const write = this.#stepTail.then(() =>
      recordStep(this.recorder.state, step, this.options.costCalculator),
    );
    this.#stepTail = write.catch((error: unknown) => {
      this.requestRecordingFailure("step", error);
    });
    await this.#stepTail;
    if (this.#recordingError !== undefined) {
      await this.finalize(false);
    }
  }

  async complete(result: unknown): Promise<void> {
    if (this.recorder.state.failure !== undefined) {
      this.requestFailure(this.recorder.state.failure);
    }
    await this.finalize(true, result);
  }

  async fail(error: unknown): Promise<void> {
    if (this.#completionStarted) {
      await this.#finalizerPromise;
      return;
    }
    this.requestFailure(error);
    await this.finalize(false);
  }

  private requestRecordingFailure(
    stage: StreamRecordingErrorStage,
    error: unknown,
  ): void {
    this.#recordingError ??= { error, stage };
    this.requestFailure(error);
  }

  private requestFailure(error: unknown): void {
    if (!this.#failureRequested) {
      this.#failureRequested = true;
      this.#failureReason = error;
    }
  }

  private async finalize(complete: boolean, result?: unknown): Promise<void> {
    this.#finalizerPromise ??= (async () => {
      await this.#stepTail;
      if (!this.#failureRequested && complete) {
        // The API cannot reopen a terminal session. Choose completion once all
        // queued steps settle; later aborts cannot reverse this terminal write.
        this.#completionStarted = true;
        try {
          await this.recorder.complete(result);
        } catch (error) {
          this.requestRecordingFailure("complete", error);
        }
      }
      if (this.#failureRequested) {
        await this.cleanup(this.#failureReason);
      }
      if (this.#recordingError) {
        this.notify(this.#recordingError.stage, this.#recordingError.error);
      }
    })();
    await this.#finalizerPromise;
  }

  private cleanup(error: unknown): Promise<void> {
    this.#cleanupPromise ??= this.recorder.fail(error).catch(() => undefined);
    return this.#cleanupPromise;
  }

  private notify(stage: StreamRecordingErrorStage, error: unknown): void {
    if (this.#notified) return;
    this.#notified = true;
    const event = {
      error,
      sessionId: this.recorder.state.sessionId,
      stage,
    };
    if (this.options.onRecordingError) {
      void Promise.resolve()
        .then(() => this.options.onRecordingError?.(event))
        .catch(() => undefined);
      return;
    }
    console.warn(
      `Kitaru stream recording failed at ${stage} for session ${event.sessionId}`,
    );
  }
}

export async function streamWithRecording({
  adapterVersion,
  agent,
  callerMessages,
  callerOptions,
  client,
  options,
  replayInput,
  requestedModelId,
  sessionName,
  startedAt,
}: StreamRecordingOptions): Promise<unknown> {
  assertStreamSupported(agent);
  const resolvedDefaults =
    typeof agent.getDefaultOptions === "function"
      ? await agent.getDefaultOptions({
          requestContext: callerOptions.requestContext,
        })
      : {};
  const defaults = isRecord(resolvedDefaults)
    ? (resolvedDefaults as RuntimeStreamOptions)
    : {};
  const { deepMerge } = await import("@mastra/core/utils");
  const effective = deepMerge(defaults, callerOptions) as RuntimeStreamOptions;
  await assertSupportedOptions(agent, effective);

  let recordedInput = replayInput;
  let lifecycle: StreamLifecycle | undefined;
  let initializePromise: Promise<StreamLifecycle> | undefined;
  const initialize = (): Promise<StreamLifecycle> => {
    initializePromise ??= (async () => {
      const { RunRecorder } = await import("@zenml-io/kitaru/adapter");
      const recorder = await RunRecorder.create({
        adapterVersion,
        agentId: options.agentId,
        agentVersionId: options.agentVersionId,
        client,
        effectiveInput: recordedInput,
        effectiveModelSettings: serializedSettings(effective.modelSettings),
        framework: "mastra",
        name: sessionName,
        requestedModelId,
        sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
        startedAt,
      });
      try {
        await recorder.initialize();
      } catch (error) {
        await recorder.fail(error).catch(() => undefined);
        throw error;
      }
      lifecycle = new StreamLifecycle(recorder, options);
      return lifecycle;
    })();
    return initializePromise;
  };

  const needsContext = hasMemoryOptions(effective);
  if (needsContext) {
    const processors =
      effective.inputProcessors ??
      (typeof agent.listConfiguredInputProcessors === "function"
        ? await agent.listConfiguredInputProcessors(effective.requestContext)
        : undefined);
    if (Array.isArray(processors)) {
      effective.inputProcessors = [
        ...processors,
        createContextProcessor(async (messages) => {
          recordedInput = createContextInput(
            replayInput,
            processors.length === 0 && effective.prepareStep === undefined
              ? messages
              : undefined,
            "This recording used memory behavior or input transformations outside history-only replay support.",
          );
          await initialize();
        }),
      ];
    }
  }
  if (!needsContext || !Array.isArray(effective.inputProcessors)) {
    await initialize();
  }

  const callerHooks = effective.hooks;
  const callerStep = effective.onStepFinish;
  const callerFinish = effective.onFinish;
  const callerError = effective.onError;
  const callerAbort = effective.onAbort;
  let modelError: unknown;
  effective.onStepFinish = async (step) => {
    const active = await initialize();
    const recordedStep =
      step.finishReason === "error" && modelError !== undefined
        ? { ...step, error: modelError }
        : step;
    await active.record(recordedStep as RecordedStep);
    if (step.finishReason === "error") modelError = undefined;
    try {
      await options.configuredOnStepFinish?.(step);
      await callerStep?.(step);
    } catch (error) {
      await active.fail(error);
      throw error;
    }
  };
  effective.onFinish = async (event) => {
    const active = await initialize();
    try {
      await callerFinish?.(event);
    } catch (error) {
      await active.fail(error);
      throw error;
    }
    const tripwire = getTripwireReason(event);
    if (tripwire) {
      await active.fail(new Error(tripwire));
      return;
    }
    await active.complete(
      recordedToolPayloadJson(
        runResultSummary(event, {
          structuredOutputField:
            effective.structuredOutput === undefined ? undefined : "object",
        }),
        "run output",
      ),
    );
  };
  effective.onError = async (event) => {
    modelError = event.error;
    const active = await initialize();
    await active.fail(event.error);
    await callerError?.(event);
  };
  effective.onAbort = async (event) => {
    const active = await initialize();
    await active.fail(new Error("Mastra stream aborted"));
    await callerAbort?.(event);
  };
  const getToolHooks = async () =>
    createToolHooks({
      callerHooks,
      configuredAfterToolCall: options.configuredAfterToolCall,
      configuredBeforeToolCall: options.configuredBeforeToolCall,
      state: (await initialize()).recorder.state,
    });
  effective.hooks = {
    beforeToolCall: async (event) =>
      (await getToolHooks()).beforeToolCall?.(event),
    afterToolCall: async (event) =>
      (await getToolHooks()).afterToolCall?.(event),
  };

  try {
    return await agent.stream(callerMessages, effective);
  } catch (error) {
    const active =
      lifecycle ?? (await initializePromise?.catch(() => undefined));
    await active?.fail(error);
    throw error;
  }
}
