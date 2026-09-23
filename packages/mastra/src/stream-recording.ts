import { createRequire } from "node:module";
import type { JsonValue, KitaruClient } from "@zenml-io/kitaru";
import {
  parseModelSettings,
  type ReplayContext,
  type RunRecorder,
  recordedToolPayloadJson,
  runResultSummary,
  serializedSettings,
  stripSystemMessages,
} from "@zenml-io/kitaru/adapter";

import {
  createContextInput,
  createContextProcessor,
  hasMemoryOptions,
  restoreConversationContext,
  unsupportedContext,
} from "./conversation-context.js";
import {
  assertReplayToolCoverage,
  stripLiveMemoryOptions,
} from "./replay-guards.js";
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
  replay: ReplayContext;
}

const ERROR_STEP_GRACE_MS = 250;
const MAX_STREAM_ERROR_NAME_LENGTH = 80;

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
  assertDisabled("prepareStep", options.prepareStep);
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

function getSafeStreamError(error: unknown): Error {
  const name =
    error instanceof Error &&
    error.name.length <= MAX_STREAM_ERROR_NAME_LENGTH &&
    /^[A-Za-z][A-Za-z0-9_]*Error$/.test(error.name)
      ? error.name
      : undefined;
  return new Error(
    name && name !== "Error"
      ? `Mastra stream failed (${name})`
      : "Mastra stream failed",
  );
}

class StreamLifecycle {
  #cleanupPromise?: Promise<void>;
  #completionStarted = false;
  #deferredFailure?: ReturnType<typeof setTimeout>;
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
    let writeFailure: { error: unknown } | undefined;
    const write = this.#stepTail.then(() =>
      recordStep(
        this.recorder.state,
        step,
        this.options.costCalculator,
        this.options.recordingLimits,
      ),
    );
    this.#stepTail = write.catch((error: unknown) => {
      writeFailure = { error };
      this.requestRecordingFailure("step", error);
    });
    await this.#stepTail;
    if (writeFailure !== undefined) {
      await this.cleanup(writeFailure.error, "recording");
    }
  }

  async complete(result: unknown): Promise<void> {
    this.cancelDeferredFailure();
    if (this.recorder.state.failure !== undefined) {
      this.requestFailure(this.recorder.state.failure);
    }
    await this.finalize(true, result);
  }

  async fail(error: unknown): Promise<void> {
    this.cancelDeferredFailure();
    if (this.#completionStarted) {
      await this.#finalizerPromise;
      return;
    }
    this.requestFailure(error);
    await this.finalize(false);
  }

  deferFailure(error: unknown): void {
    if (this.#completionStarted || this.#deferredFailure !== undefined) return;
    // Mastra normally follows onError with a failed step. Error-only paths,
    // including total timeouts, close without that callback. Give the failed
    // step a bounded chance to arrive before closing the session without it.
    this.#deferredFailure = setTimeout(() => {
      this.#deferredFailure = undefined;
      void this.fail(error).catch(() => undefined);
    }, ERROR_STEP_GRACE_MS);
  }

  cancelDeferredFailure(): void {
    if (this.#deferredFailure === undefined) return;
    clearTimeout(this.#deferredFailure);
    this.#deferredFailure = undefined;
  }

  private requestRecordingFailure(
    stage: StreamRecordingErrorStage,
    error: unknown,
  ): void {
    if (this.#recordingError !== undefined) return;
    this.#recordingError = { error, stage };
    this.notify(stage, error);
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
      if (this.#recordingError !== undefined) {
        await this.cleanup(this.#recordingError.error, "recording");
        return;
      }
      if (!this.#failureRequested && complete) {
        // The API cannot reopen a terminal session. Choose completion once all
        // queued steps settle; later aborts cannot reverse this terminal write.
        this.#completionStarted = true;
        let completionFailure: { error: unknown } | undefined;
        try {
          await this.recorder.complete(result);
        } catch (error) {
          completionFailure = { error };
          this.requestRecordingFailure("complete", error);
        }
        if (completionFailure !== undefined) {
          await this.cleanup(completionFailure.error, "recording");
        }
      }
      if (this.#failureRequested) {
        await this.cleanup(this.#failureReason, "run");
      }
    })();
    await this.#finalizerPromise;
  }

  private cleanup(error: unknown, kind: "recording" | "run"): Promise<void> {
    this.#cleanupPromise ??= (
      kind === "recording"
        ? this.recorder.failRecording(
            new Error("Kitaru stream recording failed"),
          )
        : this.recorder.fail(error)
    ).catch(() => undefined);
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
  replay,
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
  const needsContext = hasMemoryOptions(effective);
  const contextMessages = restoreConversationContext(replayInput);
  if (contextMessages && !replay.spec) {
    throw new Error(
      "A recorded Mastra conversation context can only be restored through a Kitaru replay. Start a replay for this session to keep live memory isolated.",
    );
  }
  if (replay.spec && needsContext && contextMessages === undefined) {
    throw unsupportedContext();
  }
  if (
    contextMessages &&
    (replay.override?.prompt != null || replay.override?.system_prompt != null)
  ) {
    throw new Error(
      "Unsupported Mastra replay: prompt and system_prompt overrides cannot replace a recorded conversation context. Record a new invocation with the desired messages.",
    );
  }
  let effectiveMessages = contextMessages ?? callerMessages;
  if (contextMessages) {
    effective.instructions = [];
    effective.system = [];
    effective.context = [];
  }
  let replayAbortController: AbortController | undefined;
  if (replay.replacementModelId !== undefined) {
    if (!options.resolveModel) {
      throw new Error(
        `Cannot resolve replacement model '${replay.replacementModelId}' without resolveModel`,
      );
    }
    const resolved = await options.resolveModel(replay.replacementModelId);
    if (resolved === undefined || resolved === null) {
      throw new Error(
        `Replacement model '${replay.replacementModelId}' did not resolve`,
      );
    }
    effective.model = resolved;
  }
  if (
    replay.override?.system_prompt !== undefined &&
    replay.override.system_prompt !== null
  ) {
    delete effective.system;
    effective.instructions = replay.override.system_prompt;
    effectiveMessages = stripSystemMessages(effectiveMessages);
  }
  const overrideModelSettings = parseModelSettings(
    replay.override?.model_params,
  );
  if (overrideModelSettings) {
    effective.modelSettings = {
      ...effective.modelSettings,
      ...overrideModelSettings,
    };
  }
  if (replay.spec) {
    replayAbortController = new AbortController();
    effective.abortSignal = callerOptions.abortSignal
      ? AbortSignal.any([
          callerOptions.abortSignal,
          replayAbortController.signal,
        ])
      : replayAbortController.signal;
    stripLiveMemoryOptions(effective);
    await assertReplayToolCoverage({
      agent,
      methodType: "stream",
      runtimeOptions: effective,
      spec: replay.spec,
    });
    effective.toolCallConcurrency = 1;
  }
  const processors =
    effective.inputProcessors ??
    (typeof agent.listConfiguredInputProcessors === "function"
      ? await agent.listConfiguredInputProcessors(effective.requestContext)
      : undefined);
  if (
    processors != null &&
    (!Array.isArray(processors) || processors.length > 0)
  ) {
    throw new TypeError(
      "KitaruAgent.stream() does not support user inputProcessors",
    );
  }
  await assertSupportedOptions(agent, effective);

  let recordedInput =
    needsContext && !replay.spec
      ? createContextInput(replayInput)
      : replayInput;
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
        replayId: replay.replayId,
        requestedModelId,
        sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
        startedAt,
        spec: replay.spec,
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

  if (needsContext && !replay.spec) {
    if (Array.isArray(processors)) {
      effective.inputProcessors = [
        ...processors,
        createContextProcessor(async (messages) => {
          recordedInput = createContextInput(
            replayInput,
            messages,
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
    if (step.finishReason === "error") active.cancelDeferredFailure();
    const pendingModelError =
      step.finishReason === "error" ? modelError : undefined;
    const recordedStep =
      pendingModelError !== undefined
        ? { ...step, error: getSafeStreamError(pendingModelError) }
        : step;
    await active.record(recordedStep as RecordedStep);
    if (step.finishReason === "error") modelError = undefined;
    if (replay.spec && active.recorder.state.failure !== undefined) {
      await active.fail(active.recorder.state.failure);
      return;
    }
    try {
      await options.configuredOnStepFinish?.(step);
      await callerStep?.(step);
    } catch (error) {
      await active.fail(getSafeStreamError(error));
      throw error;
    }
    if (pendingModelError !== undefined) {
      await active.fail(getSafeStreamError(pendingModelError));
    }
  };
  effective.onFinish = async (event) => {
    const active = await initialize();
    if (replay.spec && active.recorder.state.failure !== undefined) {
      await active.fail(active.recorder.state.failure);
      return;
    }
    try {
      await callerFinish?.(event);
    } catch (error) {
      await active.fail(getSafeStreamError(error));
      throw error;
    }
    const tripwire = getTripwireReason(event);
    if (tripwire) {
      await active.fail(new Error(tripwire));
      return;
    }
    const summary = runResultSummary(event, {
      structuredOutputField:
        effective.structuredOutput === undefined ? undefined : "object",
    });
    if (
      effective.structuredOutput !== undefined &&
      isRecord(summary) &&
      typeof summary.text === "string"
    ) {
      try {
        // Structured output can repeat the same credentials in its JSON text.
        summary.text = JSON.stringify(
          recordedToolPayloadJson(JSON.parse(summary.text), "run output text"),
        );
      } catch {
        // Non-JSON explanatory text retains the ordinary text recording contract.
      }
    }
    await active.complete(recordedToolPayloadJson(summary, "run output"));
  };
  effective.onError = async (event) => {
    modelError ??= event.error;
    const active =
      lifecycle ?? (await initializePromise?.catch(() => undefined));
    active?.deferFailure(
      active.recorder.state.failure ?? getSafeStreamError(modelError),
    );
    await callerError?.(event);
  };
  effective.onAbort = async (event) => {
    const active = await initialize();
    await active.fail(
      active.recorder.state.failure ?? new Error("Mastra stream aborted"),
    );
    await callerAbort?.(event);
  };
  const getToolHooks = async () =>
    createToolHooks({
      abortReplay: (reason) => replayAbortController?.abort(reason),
      callerHooks,
      configuredAfterToolCall: options.configuredAfterToolCall,
      configuredBeforeToolCall: options.configuredBeforeToolCall,
      limits: options.recordingLimits,
      state: (await initialize()).recorder.state,
    });
  effective.hooks = {
    beforeToolCall: async (event) =>
      (await getToolHooks()).beforeToolCall?.(event),
    afterToolCall: async (event) =>
      (await getToolHooks()).afterToolCall?.(event),
  };

  try {
    return await agent.stream(effectiveMessages, effective);
  } catch (error) {
    const active =
      lifecycle ?? (await initializePromise?.catch(() => undefined));
    const replayFailure = replay.spec
      ? active?.recorder.state.failure
      : undefined;
    await active?.fail(replayFailure ?? getSafeStreamError(error));
    throw replayFailure ?? error;
  }
}
