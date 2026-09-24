import { createRequire } from "node:module";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  type AdapterClient,
  type AdapterRunState,
  parseModelSettings,
  type ReplayContext,
  ROOT_NODE_EXTERNAL_ID,
  type RunRecorder,
  recordedToolPayloadJson,
  recordNormalizedStep,
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
import { MastraOMDivergenceError } from "./om-result-tape.js";
import { describeProviderError } from "./provider-errors.js";
import {
  assertReplayToolCoverage,
  stripLiveMemoryOptions,
} from "./replay-guards.js";
import {
  createIneligibleMetadata,
  getReplayReason,
  type MastraReplayReason,
  MastraReplayReasonError,
} from "./replay-reasons.js";
import type { RequestEvidence } from "./request-capture.js";
import { normalizeStep, type RecordedStep } from "./step-recorder.js";
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

/** How a turn that ran natively, without Kitaru recording it, ended. */
export type NativeOutcome = "completed" | "failed";

/** A turn run natively after its recording could not start. */
export interface NativeFallbackRun {
  result: unknown;
  /** Settles once the native turn ends; it may never settle if the turn never ends. */
  outcome: Promise<NativeOutcome>;
}

export interface StatefulStreamRecording {
  input: JsonValue;
  sanitizeEvidence?: <T>(value: T) => T;
  initialize(state: AdapterRunState): void;
  /**
   * Take the request evidence of the step that just finished. A failed step
   * may take the attempt whose provider call threw.
   */
  takeRequest(failedStep: boolean): RequestEvidence | undefined;
  /** Start joining the invocation's background memory work without waiting. */
  beginFinalization?(): void;
  /**
   * Receive a native tripwire that ends the run without Mastra calling
   * `onFinish` or `onError`, so the session can still be closed.
   */
  setTripwireListener?(listener: (reason: string) => void): void;
  finish(): Promise<JsonValue>;
  /** Session metadata for a completed replay, read after `finish()`. */
  getReplayMetadata?(): Record<string, JsonValue> | undefined;
  release(): Promise<void>;
}

interface StreamRecordingOptions {
  stateful?: StatefulStreamRecording;
  adapterVersion: string;
  agent: StreamAgent;
  callerMessages: unknown;
  callerOptions: RuntimeStreamOptions;
  client: AdapterClient;
  options: KitaruAgentOptions;
  replayInput: JsonValue;
  requestedModelId: string;
  sessionName?: string;
  startedAt: string;
  replay: ReplayContext;
  /**
   * Run the turn natively when recording fails before the model starts.
   * `reason` says why the turn is not recorded.
   */
  nativeFallback?: (
    error: unknown,
    reason: MastraReplayReason,
  ) => Promise<NativeFallbackRun>;
  markNativeStart?: () => void;
  /** How a native fallback turn ended, once one has run. */
  fallbackOutcome?: Promise<NativeOutcome>;
  /** Called when Kitaru could not open the recording session. */
  onRecordingNotOpened?: () => void;
  /**
   * How long a baseline waits for Kitaru to open its session before the
   * stream starts. When it runs out, the stream fails over to `nativeFallback`
   * and a session that opens late is closed as ineligible. Unset waits for the
   * client's own timeout.
   */
  setupWaitMs?: number;
  /**
   * How long a baseline's finalization waits, from its start, for queued
   * evidence uploads. When it runs out, the uploads are cancelled and the
   * session is closed as ineligible. Unset waits for every upload.
   */
  flushWaitMs?: number;
}

const ERROR_STEP_GRACE_MS = 250;
const SETUP_TIMEOUT = "Kitaru did not open the recording session in time.";
const FLUSH_TIMEOUT = "Kitaru did not accept the recording evidence in time.";

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

const OM_DIVERGED = "KITARU_REPLAY_DIVERGED:mastra_om_call_order";

/**
 * Send every evidence node upload with `signal`, so aborting it cancels queued
 * and in-flight evidence. Root span upserts open and close the session and
 * keep only the client's own timeout.
 */
function withCancelableEvidence(
  client: AdapterClient,
  signal: AbortSignal,
): AdapterClient {
  return {
    createSession: (request, options) => client.createSession(request, options),
    getReplay: (replayId, options) => client.getReplay(replayId, options),
    getTaskSpec: (taskId, options) => client.getTaskSpec(taskId, options),
    lookupToolResult: (replayId, request, options) =>
      client.lookupToolResult(replayId, request, options),
    updateSession: (sessionId, request, options) =>
      client.updateSession(sessionId, request, options),
    upsertSessionNodes: (sessionId, request, options) =>
      client.upsertSessionNodes(
        sessionId,
        request,
        request.nodes.some((node) => node.external_id !== ROOT_NODE_EXTERNAL_ID)
          ? {
              ...options,
              signal: options?.signal
                ? AbortSignal.any([options.signal, signal])
                : signal,
            }
          : options,
      ),
  };
}

function getTripwireReason(value: unknown): string | undefined {
  if (!isRecord(value) || !isRecord(value.tripwire)) return undefined;
  return typeof value.tripwire.reason === "string" && value.tripwire.reason
    ? value.tripwire.reason
    : "Mastra processor tripwire triggered";
}

/** A stream failure whose message is already safe to record. */
class SafeStreamError extends Error {}

function getSafeStreamError(error: unknown): Error {
  if (error instanceof SafeStreamError) return error;
  if (
    error instanceof MastraOMDivergenceError ||
    (error instanceof Error &&
      (error.message.includes(
        "Recorded Mastra observational memory diverged:",
      ) ||
        error.message === OM_DIVERGED))
  )
    return new SafeStreamError(OM_DIVERGED);
  const detail = describeProviderError(error);
  return new SafeStreamError(
    detail ? `Mastra stream failed (${detail})` : "Mastra stream failed",
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
    reasonCode: MastraReplayReason;
  };
  #nativeState: "pending" | "completed" | "failed" = "pending";
  // Steps are converted and queued in order here; the uploads themselves run
  // in the run state's queue, and the last one settles after every earlier one.
  #stepTail: Promise<void> = Promise.resolve();
  #uploadTail: Promise<void> = Promise.resolve();

  constructor(
    readonly recorder: RunRecorder,
    readonly options: KitaruAgentOptions,
    readonly stateful?: StatefulStreamRecording,
    readonly evidenceFlush?: { waitMs: number; cancel(): void },
  ) {}

  /** Queue a step's upload without waiting for Kitaru to accept it. */
  record(step: RecordedStep): void {
    if (this.#recordingError !== undefined) return;
    // Take the step's request evidence and end time now: the conversion can
    // run after the next step has started.
    const request = this.stateful?.takeRequest(step.finishReason === "error");
    const endedAt = new Date().toISOString();
    const queued = this.#stepTail.then(async () => {
      const normalized = await normalizeStep(
        this.recorder.state,
        step,
        this.options.costCalculator,
        this.options.recordingLimits,
        request,
        this.stateful?.sanitizeEvidence,
        endedAt,
      );
      this.#uploadTail = recordNormalizedStep(
        this.recorder.state,
        normalized,
      ).catch((error: unknown) => {
        this.requestRecordingFailure("step", error, "recording_step_failed");
      });
    });
    this.#stepTail = queued.catch((error: unknown) => {
      this.requestRecordingFailure("step", error, "recording_step_failed");
    });
  }

  async complete(result: unknown): Promise<void> {
    this.cancelDeferredFailure();
    if (this.recorder.state.failure !== undefined) {
      this.requestFailure(this.recorder.state.failure);
    }
    await this.settle(this.finalize(true, result));
  }

  async fail(error: unknown): Promise<void> {
    this.cancelDeferredFailure();
    if (this.#completionStarted) {
      await this.settle(this.#finalizerPromise ?? Promise.resolve());
      return;
    }
    this.requestFailure(error);
    await this.settle(this.finalize(false));
  }

  /**
   * Wait for finalization, or leave a baseline memory recording to finish in
   * the background.
   */
  private async settle(finalization: Promise<void>): Promise<void> {
    if (this.stateful && !this.recorder.state.spec) {
      // Mastra can start observational work after the actor finishes, and
      // Kitaru uploads can be slow. Keep the recorder and source lease alive
      // without delaying the native stream.
      void finalization.catch((error: unknown) => {
        this.requestRecordingFailure(
          "complete",
          error,
          "recording_finalization_failed",
        );
      });
      return;
    }
    await finalization;
  }

  markNativeCompleted(): void {
    if (this.#nativeState === "pending") this.#nativeState = "completed";
  }

  markNativeFailed(): void {
    this.#nativeState = "failed";
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
    reasonCode: MastraReplayReason,
  ): void {
    if (this.#recordingError !== undefined) return;
    this.#recordingError = { error, stage, reasonCode };
    this.notify(stage, error, reasonCode);
  }

  /** Whether this lifecycle records a memory baseline, which has a replay state. */
  private get recordsBaseline(): boolean {
    return this.stateful !== undefined && !this.recorder.state.spec;
  }

  private requestFailure(error: unknown): void {
    if (!this.#failureRequested) {
      this.#failureRequested = true;
      this.#failureReason = error;
    }
  }

  private async finalize(complete: boolean, result?: unknown): Promise<void> {
    this.#finalizerPromise ??= (async () => {
      let flushDeadline: ReturnType<typeof setTimeout> | undefined;
      try {
        // Start before the first await so a caller that joins background work
        // right after the stream closes already sees this invocation's work.
        this.stateful?.beginFinalization?.();
        const flush = this.evidenceFlush;
        if (flush)
          flushDeadline = setTimeout(() => {
            this.requestRecordingFailure(
              "complete",
              new Error(FLUSH_TIMEOUT),
              "recording_flush_timeout",
            );
            flush.cancel();
          }, flush.waitMs);
        // Queue every step upload before the stateful finish queues its own
        // evidence, so the run state uploads them in step order.
        await this.#stepTail;
        let finalInput: JsonValue | undefined;
        try {
          finalInput = await this.stateful?.finish();
        } catch (error) {
          if (
            this.recorder.state.spec &&
            error instanceof MastraOMDivergenceError
          )
            this.requestFailure(getSafeStreamError(error));
          else
            this.requestRecordingFailure(
              "complete",
              error,
              getReplayReason(error, "recording_finalization_failed"),
            );
        }
        await this.#uploadTail;
        clearTimeout(flushDeadline);
        if (this.#failureRequested) {
          await this.cleanup(this.#failureReason, "run");
          return;
        }
        if (this.#recordingError !== undefined) {
          // The native answer succeeded; only its recording is unusable.
          if (complete && this.recordsBaseline)
            await this.completeIneligible(
              result,
              this.#recordingError.reasonCode,
            );
          else await this.cleanup(this.#recordingError.error, "recording");
          return;
        }
        if (!this.#failureRequested && complete) {
          this.#completionStarted = true;
          const replayMetadata = this.recorder.state.spec
            ? this.stateful?.getReplayMetadata?.()
            : undefined;
          try {
            const completion = await this.recorder.complete(
              result,
              this.recordsBaseline
                ? {
                    inputs: finalInput,
                    metadata: {
                      mastra_replay_state: "eligible",
                      mastra_native_state: "completed",
                    },
                    rejectedMetadata: createIneligibleMetadata(
                      "server_rejected_finalization",
                      "completed",
                    ),
                  }
                : replayMetadata
                  ? { metadata: replayMetadata }
                  : undefined,
            );
            if (this.recordsBaseline && !completion.finalizationAccepted)
              this.notify(
                "complete",
                new Error(
                  "Kitaru refused the replay inputs, so the session is stored without them.",
                ),
                "server_rejected_finalization",
              );
          } catch (error) {
            this.requestRecordingFailure(
              "complete",
              error,
              "recording_completion_failed",
            );
            await this.cleanup(error, "recording");
          }
        }
        if (this.#failureRequested)
          await this.cleanup(this.#failureReason, "run");
      } finally {
        clearTimeout(flushDeadline);
        await this.stateful
          ?.release()
          .catch((error: unknown) => this.notify("complete", error));
      }
    })();
    await this.#finalizerPromise;
  }

  /** Complete a baseline whose native answer succeeded but whose recording is unusable. */
  private async completeIneligible(
    result: unknown,
    reasonCode: MastraReplayReason,
  ): Promise<void> {
    this.#completionStarted = true;
    try {
      await this.recorder.completeIncompleteRecording(
        result,
        createIneligibleMetadata(reasonCode, "completed"),
      );
    } catch (error) {
      await this.cleanup(error, "recording");
    }
  }

  private cleanup(error: unknown, kind: "recording" | "run"): Promise<void> {
    this.#cleanupPromise ??= (async () => {
      const reasonCode = this.#recordingError?.reasonCode;
      const safeError = getSafeStreamError(error);
      const omDiverged = safeError.message === OM_DIVERGED;
      const metadata: Record<string, JsonValue> | undefined = this
        .recordsBaseline
        ? createIneligibleMetadata(
            reasonCode ??
              (kind === "run"
                ? "native_run_failed"
                : "recording_finalization_failed"),
            this.#nativeState,
          )
        : this.stateful && this.recorder.state.spec
          ? {
              mastra_replay_state: omDiverged ? "diverged" : "failed",
              mastra_replay_reason: omDiverged
                ? "mastra_om_call_order"
                : "replay_failed",
            }
          : undefined;
      if (kind === "recording") {
        await this.recorder.failRecording(
          new Error(`KITARU_RECORDING_INCOMPLETE:${reasonCode ?? "unknown"}`),
          metadata,
        );
      } else {
        await this.recorder.fail(
          reasonCode
            ? new Error(
                `${safeError.message}; KITARU_RECORDING_INCOMPLETE:${reasonCode}`,
              )
            : error,
          metadata,
        );
      }
    })().catch(() => undefined);
    return this.#cleanupPromise;
  }

  private notify(
    stage: StreamRecordingErrorStage,
    error: unknown,
    reason?: MastraReplayReason,
  ): void {
    if (this.#notified) return;
    this.#notified = true;
    const event = {
      error,
      sessionId: this.recorder.state.sessionId,
      stage,
      ...(reason === undefined ? {} : { reason }),
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

async function recordedStreamWithRecording({
  stateful,
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
  markNativeStart,
  fallbackOutcome,
  onRecordingNotOpened,
  setupWaitMs,
  flushWaitMs,
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
  const needsContext = !stateful && hasMemoryOptions(effective);
  const contextMessages = stateful
    ? undefined
    : restoreConversationContext(replayInput);
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
  if (!stateful && replay.replacementModelId !== undefined) {
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
    !stateful &&
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
    if (!stateful) {
      stripLiveMemoryOptions(effective);
      await assertReplayToolCoverage({
        agent,
        methodType: "stream",
        runtimeOptions: effective,
        spec: replay.spec,
      });
    }
    effective.toolCallConcurrency = 1;
  }
  const processors =
    effective.inputProcessors ??
    (typeof agent.listConfiguredInputProcessors === "function"
      ? await agent.listConfiguredInputProcessors(effective.requestContext)
      : undefined);
  if (
    !stateful &&
    processors != null &&
    (!Array.isArray(processors) || processors.length > 0)
  ) {
    throw new TypeError(
      "KitaruAgent.stream() does not support user inputProcessors",
    );
  }
  await assertSupportedOptions(agent, effective);

  let recordedInput =
    stateful?.input ??
    (needsContext && !replay.spec
      ? createContextInput(replayInput)
      : replayInput);
  let lifecycle: StreamLifecycle | undefined;
  let initializePromise: Promise<StreamLifecycle> | undefined;
  const evidenceUploads = new AbortController();
  const recordingClient = withCancelableEvidence(
    client,
    evidenceUploads.signal,
  );
  let setupAbandoned = false;
  const openRecording = async (): Promise<StreamLifecycle> => {
    const { RunRecorder } = await import("@zenml-io/kitaru/adapter");
    const recorder = await RunRecorder.create({
      adapterVersion,
      agentId: options.agentId,
      agentVersionId: options.agentVersionId,
      client: recordingClient,
      effectiveInput: recordedInput,
      effectiveModelSettings: serializedSettings(effective.modelSettings),
      framework: "mastra",
      ...(stateful && !replay.spec
        ? {
            metadata: {
              mastra_replay_state: "pending",
              mastra_native_state: "pending",
            },
          }
        : {}),
      name: sessionName,
      replayId: replay.replayId,
      requestedModelId,
      sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
      startedAt,
      spec: replay.spec,
    });
    const baseline = stateful !== undefined && !replay.spec;
    // A baseline fails over to its native fallback, so its session records
    // how that native turn ended.
    const closeUnopened = (reasonCode: MastraReplayReason): Promise<void> =>
      (async () => {
        const native = baseline ? await fallbackOutcome : undefined;
        if (native === "completed") {
          await recorder.completeIncompleteRecording(
            null,
            createIneligibleMetadata(reasonCode, native),
          );
          return;
        }
        await recorder.failRecording(
          new Error(`KITARU_RECORDING_INCOMPLETE:${reasonCode}`),
          baseline
            ? createIneligibleMetadata(reasonCode, native ?? "pending")
            : undefined,
        );
      })().catch(() => undefined);
    if (!setupAbandoned) {
      try {
        await recorder.initialize();
      } catch (error) {
        const closing = closeUnopened(
          setupAbandoned ? "recording_setup_timeout" : "recording_setup_failed",
        );
        // The native fallback starts only after this throw, so a baseline
        // cannot wait here for its outcome.
        if (!baseline) await closing;
        throw error;
      }
    }
    // The turn has already failed over to its native fallback, so a session
    // that opens late must not stay pending.
    if (setupAbandoned) {
      void closeUnopened("recording_setup_timeout");
      throw new MastraReplayReasonError(
        SETUP_TIMEOUT,
        "recording_setup_timeout",
      );
    }
    stateful?.initialize(recorder.state);
    const active = new StreamLifecycle(
      recorder,
      options,
      stateful,
      flushWaitMs === undefined || replay.spec
        ? undefined
        : {
            waitMs: flushWaitMs,
            cancel: () => evidenceUploads.abort(new Error(FLUSH_TIMEOUT)),
          },
    );
    lifecycle = active;
    stateful?.setTripwireListener?.((reason) => {
      active.markNativeFailed();
      const error = new Error(reason);
      const safe = getSafeStreamError(error);
      void active
        .fail(safe.message === OM_DIVERGED ? safe : error)
        .catch(() => undefined);
    });
    return active;
  };
  const initialize = (): Promise<StreamLifecycle> => {
    initializePromise ??= (async () => {
      const setup = openRecording().catch((error: unknown) => {
        onRecordingNotOpened?.();
        throw error;
      });
      if (setupWaitMs === undefined || replay.spec) return setup;
      void setup.catch(() => undefined);
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          setup,
          new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => {
              setupAbandoned = true;
              reject(
                new MastraReplayReasonError(
                  SETUP_TIMEOUT,
                  "recording_setup_timeout",
                ),
              );
            }, setupWaitMs);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
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
    if (step.finishReason === "error") {
      active.cancelDeferredFailure();
      active.markNativeFailed();
    }
    const pendingModelError =
      step.finishReason === "error" ? modelError : undefined;
    const recordedStep =
      pendingModelError !== undefined
        ? { ...step, error: getSafeStreamError(pendingModelError) }
        : step;
    active.record(recordedStep as RecordedStep);
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
      active.markNativeFailed();
      await active.fail(getSafeStreamError(error));
      throw error;
    }
    active.markNativeCompleted();
    const tripwire = getTripwireReason(event);
    if (tripwire) {
      await active.fail(new Error(tripwire));
      return;
    }
    const rawSummary = runResultSummary(event, {
      structuredOutputField:
        effective.structuredOutput === undefined ? undefined : "object",
    });
    const summary = stateful?.sanitizeEvidence?.(rawSummary) ?? rawSummary;
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
    active?.markNativeFailed();
    active?.deferFailure(
      active.recorder.state.failure ?? getSafeStreamError(modelError),
    );
    await callerError?.(event);
  };
  effective.onAbort = async (event) => {
    const active = await initialize();
    active.markNativeFailed();
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
  if (!stateful)
    effective.hooks = {
      beforeToolCall: async (event) =>
        (await getToolHooks()).beforeToolCall?.(event),
      afterToolCall: async (event) =>
        (await getToolHooks()).afterToolCall?.(event),
    };

  try {
    markNativeStart?.();
    return await agent.stream(effectiveMessages, effective);
  } catch (error) {
    const active =
      lifecycle ?? (await initializePromise?.catch(() => undefined));
    active?.markNativeFailed();
    const replayFailure = replay.spec
      ? active?.recorder.state.failure
      : undefined;
    await active?.fail(replayFailure ?? getSafeStreamError(error));
    throw replayFailure ?? error;
  }
}

export async function streamWithRecording(
  options: StreamRecordingOptions,
): Promise<unknown> {
  let nativeStarted = false;
  let recordingOpened = true;
  let reportOutcome: (outcome: Promise<NativeOutcome>) => void = () => {};
  const fallbackOutcome = new Promise<NativeOutcome>((resolve) => {
    reportOutcome = (outcome) => {
      outcome.then(resolve, () => resolve("failed"));
    };
  });
  try {
    return await recordedStreamWithRecording({
      ...options,
      markNativeStart: () => {
        nativeStarted = true;
      },
      fallbackOutcome: options.nativeFallback ? fallbackOutcome : undefined,
      onRecordingNotOpened: () => {
        recordingOpened = false;
      },
    });
  } catch (error) {
    if (nativeStarted || options.replay.spec || !options.nativeFallback)
      throw error;
    let run: NativeFallbackRun;
    try {
      run = await options.nativeFallback(
        error,
        getReplayReason(
          error,
          recordingOpened
            ? "agent_config_unsupported"
            : "recording_setup_failed",
        ),
      );
    } catch (fallbackError) {
      reportOutcome(Promise.resolve("failed"));
      throw fallbackError;
    }
    reportOutcome(run.outcome);
    return run.result;
  }
}
