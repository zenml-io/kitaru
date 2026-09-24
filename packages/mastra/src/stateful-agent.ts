import { createRequire } from "node:module";
import { Agent, type AgentConfig } from "@mastra/core/agent";
import type { MastraModelConfig } from "@mastra/core/llm";
import type { Mastra } from "@mastra/core/mastra";
import type { MemoryConfigInternal } from "@mastra/core/memory";
import type { InputProcessor } from "@mastra/core/processors";
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from "@mastra/core/request-context";
import type { MemoryStorage } from "@mastra/core/storage";
import type { Memory } from "@mastra/memory";
import {
  type JsonValue,
  KitaruClient,
  type SessionNodeCreateRequest,
} from "@zenml-io/kitaru";
import {
  type AdapterClient,
  type AdapterRunState,
  normalizeRecordingLimits,
  parseModelSettings,
  ROOT_NODE_EXTERNAL_ID,
  resolveReplayContext,
} from "@zenml-io/kitaru/adapter";
import {
  createMemoryCaptureBinding,
  createRegisteredWriteDomain,
  type MastraExclusiveMemoryAccess,
  type MastraMemoryCaptureBinding,
  type MastraMemoryMutation,
  type MastraMemorySelector,
} from "./memory-binding.js";
import {
  assertMemoryReplayVersions,
  bindOMResultModels,
  createIsolatedMemoryReplay,
  getMemoryModelId,
  getMemoryStoreSemantics,
  type MastraMemoryStoreSemantics,
  pinMemoryClock,
  serializeMemoryConfiguration,
} from "./memory-replay.js";
import {
  captureMemoryReplayEnvelope,
  createIncompleteMemoryReplayEnvelope,
  decodeMemoryValue,
  encodeMemoryValue,
  finalizeMemoryReplayEnvelope,
  type MastraMemorySnapshot,
  MEMORY_REPLAY_KEY,
  restoreMemoryReplayEnvelope,
  validateMemoryReplayContext,
  validateMemoryReplaySelectors,
} from "./memory-snapshot.js";
import { createOMResultTape, type OMResultEntry } from "./om-result-tape.js";
import { describeProviderError } from "./provider-errors.js";
import { createRecordedClock } from "./replay-clock.js";
import { assertStableToolName } from "./replay-guards.js";
import {
  getReplayReason,
  type MastraReplayReason,
  MastraReplayReasonError,
} from "./replay-reasons.js";
import {
  createRequestCapture,
  type RequestEvidence,
  requestEvidenceAttributes,
} from "./request-capture.js";
import {
  createCapturedFiles,
  createFileDownloads,
  createRecordedEvidenceSanitizer,
  FileCaptureTimeoutError,
  restoreCapturedFiles,
} from "./stateful-files.js";
import {
  bindMemoryToolIdentity,
  createStatefulToolProcessors,
  reportMemoryProcessorTripwires,
} from "./stateful-tools.js";
import { loadSkillsWorkspace } from "./stateful-workspace.js";
import {
  type NativeFallbackRun,
  type NativeOutcome,
  streamWithRecording,
} from "./stream-recording.js";
import type {
  KitaruAgentOptions,
  RuntimeStreamOptions,
  StreamRecordingErrorStage,
} from "./types.js";

interface MastraMemorySource {
  settled(): Promise<void>;
  /**
   * The source Memory instance. When supplied, its `settled()` also joins the
   * memory work each recorded turn runs on its own Memory instance.
   */
  memory?: Memory;
  domain: MemoryStorage;
  configuration: MemoryConfigInternal;
  exclusiveAccess: MastraExclusiveMemoryAccess;
}

function recordingClient(
  client: KitaruClient,
  sanitize: <T>(value: T) => T,
  unsafeReason: () => string | undefined,
): AdapterClient {
  return {
    createSession: (request, options) =>
      client.createSession(sanitize(request), options),
    getReplay: client.getReplay.bind(client),
    getTaskSpec: client.getTaskSpec.bind(client),
    lookupToolResult: client.lookupToolResult.bind(client),
    upsertSessionNodes: (sessionId, request, options) =>
      client.upsertSessionNodes(sessionId, sanitize(request), options),
    updateSession: (sessionId, request, options) => {
      const safe = sanitize(request);
      const reason = unsafeReason();
      if (reason && safe.metadata?.mastra_replay_state === "eligible") {
        safe.metadata = {
          ...safe.metadata,
          mastra_replay_state: "ineligible",
          mastra_replay_reason: reason,
        };
      }
      return client.updateSession(sessionId, safe, options);
    },
  };
}

export interface MemoryReplayAgentOptions extends KitaruAgentOptions {
  /** Registry context passed to baseline dynamic configuration resolvers. */
  mastra?: Mastra;
  /** Called only for new recordings. All writers must share exclusiveAccess. */
  sourceMemory(): MastraMemorySource | Promise<MastraMemorySource>;
  /** Return only approved replay-relevant JSON context. Credentials are forbidden. */
  captureRequestContext?(context: RequestContext): Record<string, unknown>;
  /**
   * File URLs the factory's `resolveFile` may fetch during a recorded turn,
   * either fixed for the wrapper or computed for each call. Each is fetched
   * once per turn and replay serves the captured bytes.
   */
  files?: readonly string[] | DeclareMemoryReplayFiles;
  resolveFile?: (
    url: string,
  ) => Promise<{ bytes: Uint8Array; mediaType: string }>;
  skillsDirectory?: string;
  resolveModel: (id: string) => MastraModelConfig | Promise<MastraModelConfig>;
  /**
   * How long a turn waits after its stream closes for observational-memory
   * work. A baseline then releases the source lease, and a turn whose work did
   * not settle in time is recorded as ineligible; a replay whose work did not
   * settle fails. A baseline's evidence uploads to Kitaru run in the
   * background and must finish within twice this wait of the stream closing,
   * or they are cancelled and the turn is recorded as ineligible. Defaults to
   * 60 seconds.
   */
  finalizationWaitMs?: number;
  /**
   * How long a baseline turn waits for Kitaru to open its session before the
   * model starts. When Kitaru has not answered in time, the turn runs natively
   * and is not replayable. Defaults to 2 seconds.
   */
  sessionSetupWaitMs?: number;
  /**
   * How long a baseline turn waits for its declared files to download before
   * the model starts. When a download has not finished in time, the turn runs
   * natively, its file resolver takes over the running downloads, and the turn
   * is not replayable. Defaults to 10 seconds.
   */
  fileCaptureWaitMs?: number;
}

/** The call a per-call `files` function declares file URLs for. */
export interface MemoryReplayFileCall {
  input: unknown;
  options: RuntimeStreamOptions;
}

export type DeclareMemoryReplayFiles = (
  call: MemoryReplayFileCall,
) => readonly string[] | Promise<readonly string[]>;

export interface MemoryReplayAgentBindings {
  memory: Memory;
  resolveFile(url: string): Promise<{ bytes: Uint8Array; mediaType: string }>;
  workspace?: Awaited<ReturnType<typeof loadSkillsWorkspace>>["workspace"];
}

type FileDownloads = ReturnType<typeof createFileDownloads>;

export type MemoryReplayAgentFactory = (
  bindings: MemoryReplayAgentBindings,
) => AgentConfig | Promise<AgentConfig>;

const DEFAULT_FINALIZATION_WAIT_MS = 60_000;
const DEFAULT_SESSION_SETUP_WAIT_MS = 2_000;
const DEFAULT_FILE_CAPTURE_WAIT_MS = 10_000;
const CAPTURE_BUFFERING_WAIT_MS = 5_000;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!record(value))
    throw new MastraReplayReasonError(
      `Unsupported Mastra memory replay: missing ${label}.`,
      "agent_config_unsupported",
    );
  return value;
}
class MemoryReplayContextError extends MastraReplayReasonError {
  constructor(
    message: string,
    reason: MastraReplayReason = "context_unsupported",
  ) {
    super(message, reason);
  }
}

/** Reject an agent or run configuration outside isolated replay support. */
function unsupportedAgentConfiguration(message: string): never {
  throw new MastraReplayReasonError(message, "agent_config_unsupported");
}

/**
 * Pass a native turn's callbacks through and report how the turn ended.
 *
 * The caller's callbacks run first, so a callback that throws marks the turn
 * failed as it does for a recorded turn.
 */
function observeNativeOutcome(callerOptions: RuntimeStreamOptions): {
  options: RuntimeStreamOptions;
  outcome: Promise<NativeOutcome>;
} {
  let settle: (outcome: NativeOutcome) => void = () => {};
  const outcome = new Promise<NativeOutcome>((resolve) => {
    settle = resolve;
  });
  const failed = async <T>(
    callback: ((event: T) => Promise<void> | void) | undefined,
    event: T,
  ) => {
    settle("failed");
    await callback?.(event);
  };
  return {
    outcome,
    options: {
      ...callerOptions,
      onFinish: async (event) => {
        try {
          await callerOptions.onFinish?.(event);
        } catch (error) {
          settle("failed");
          throw error;
        }
        settle(record(event.tripwire) ? "failed" : "completed");
      },
      onError: (event) => failed(callerOptions.onError, event),
      onAbort: (event) => failed(callerOptions.onAbort, event),
    },
  };
}

function getSelector(options: RuntimeStreamOptions) {
  const memory = record(options.memory) ? options.memory : undefined;
  if (!memory)
    throw new MemoryReplayContextError(
      "Memory replay requires explicit memory.thread and memory.resource strings.",
    );
  const threadId =
    typeof memory.thread === "string"
      ? memory.thread
      : record(memory.thread)
        ? memory.thread.id
        : undefined;
  if (typeof threadId !== "string" || typeof memory.resource !== "string")
    throw new MemoryReplayContextError(
      "Memory replay requires explicit memory.thread and memory.resource strings.",
    );
  return { threadId, resourceId: memory.resource };
}
function assertSupportedConfiguration(
  config: AgentConfig,
  options: RuntimeStreamOptions,
): void {
  for (const name of [
    "agents",
    "workflows",
    "voice",
    "browser",
    "backgroundTasks",
    "editor",
    "defaultGenerateOptionsLegacy",
    "defaultStreamOptionsLegacy",
    "outputProcessors",
    "errorProcessors",
    "hooks",
  ]) {
    if ((config as unknown as Record<string, unknown>)[name] !== undefined)
      unsupportedAgentConfiguration(
        `Unsupported memory replay configuration '${name}'.`,
      );
  }
  for (const name of [
    "inputProcessors",
    "outputProcessors",
    "hooks",
    "toolsets",
    "clientTools",
    "prepareStep",
    "instructions",
    "model",
    "requestContext",
    "abortSignal",
    "onFinish",
    "onError",
    "onAbort",
    "onStepFinish",
  ]) {
    if (options[name] !== undefined)
      unsupportedAgentConfiguration(
        `Unsupported serialized memory replay option '${name}'.`,
      );
  }
  for (const name of Object.keys(config.tools ?? {}))
    assertStableToolName(name);
  for (const name of ["experimental_sandbox", "delegation", "backgroundTasks"])
    if (options[name] !== undefined)
      unsupportedAgentConfiguration(
        `Unsupported memory replay option ${name}.`,
      );
  if (
    typeof config.tools === "function" ||
    typeof config.inputProcessors === "function" ||
    typeof config.workspace === "function"
  )
    unsupportedAgentConfiguration(
      "Memory replay requires static tools, processors and supplied workspace bindings.",
    );
  if (
    config.inputProcessors?.some(
      (processor) =>
        !record(processor) ||
        "loadTools" in processor ||
        "createRun" in processor,
    )
  )
    unsupportedAgentConfiguration(
      "Memory replay processors must use the supplied dependencies and ordinary processor methods.",
    );
}

class MemoryReplayRequestContext extends RequestContext {
  constructor(
    private readonly selector: Pick<
      MastraMemorySnapshot,
      "threadId" | "resourceId"
    >,
  ) {
    super();
  }

  override set(key: string, value: unknown): void {
    validateMemoryReplayContext(this.selector, { [key]: value });
    super.set(key, value);
  }

  override setRaw(key: string, value: unknown): void {
    this.set(key, value);
  }
}

/** Construct each streamed invocation with historical configuration and isolated replay memory. */
export function createMemoryReplayAgent(
  factory: MemoryReplayAgentFactory,
  supplied: MemoryReplayAgentOptions,
): Pick<Agent, "stream"> {
  const options = {
    ...supplied,
    recordingLimits: normalizeRecordingLimits(supplied.recordingLimits),
  };
  const finalizationWaitMs =
    supplied.finalizationWaitMs ?? DEFAULT_FINALIZATION_WAIT_MS;
  const client = new KitaruClient({
    apiKey: options.apiKey,
    apiUrl: options.apiUrl,
    timeoutMs: options.timeoutMs,
  });
  async function runNativeBaseline(
    rawInput: unknown,
    callerOptions: RuntimeStreamOptions,
    downloads: FileDownloads,
  ): Promise<NativeFallbackRun> {
    const { MastraCompositeStore } = await import("@mastra/core/storage");
    const { Memory } = await import("@mastra/memory");
    const source = await options.sourceMemory();
    // Mastra writes only once the stream runs, after the factory has resolved
    // the default memory selectors below.
    let selector: MastraMemorySelector | undefined;
    const domain = createRegisteredWriteDomain(
      source.domain,
      source.exclusiveAccess,
      () => selector,
      (reason) =>
        reportLocalRecordingError(
          new Error(reason),
          "memory_lease_unavailable",
          "complete",
        ),
    );
    const memory = new Memory({
      storage: new MastraCompositeStore({
        id: `kitaru-native-${globalThis.crypto.randomUUID()}`,
        domains: { memory: domain },
      }),
      options: source.configuration,
    });
    const workspace = options.skillsDirectory
      ? await loadSkillsWorkspace(options.skillsDirectory)
      : undefined;
    const config = await factory({
      memory,
      resolveFile: downloads.resolveNative,
      workspace: workspace?.workspace,
    });
    selector = await getNativeSelector(config, callerOptions);
    const native = new Agent({ ...config, memory }) as unknown as {
      stream(input: unknown, options: RuntimeStreamOptions): Promise<unknown>;
    };
    const observed = observeNativeOutcome(callerOptions);
    return {
      result: await native.stream(rawInput, observed.options),
      outcome: observed.outcome,
    };
  }

  /**
   * Resolve the selectors a native call writes, or undefined when unknown.
   *
   * Follows Mastra's precedence: the reserved request-context keys override
   * the merged default and caller memory options.
   */
  async function getNativeSelector(
    config: AgentConfig,
    callerOptions: RuntimeStreamOptions,
  ): Promise<MastraMemorySelector | undefined> {
    try {
      const requestContext =
        callerOptions.requestContext ?? new RequestContext();
      const defaults = requireRecord(
        typeof config.defaultOptions === "function"
          ? await config.defaultOptions({
              requestContext,
              mastra: options.mastra,
            })
          : (config.defaultOptions ?? {}),
        "default options",
      );
      const { deepMerge } = await import("@mastra/core/utils");
      const merged = deepMerge(
        record(defaults.memory) ? defaults.memory : {},
        record(callerOptions.memory) ? callerOptions.memory : {},
      );
      const contextThread = requestContext.get(MASTRA_THREAD_ID_KEY);
      const contextResource = requestContext.get(MASTRA_RESOURCE_ID_KEY);
      return getSelector({
        memory: {
          ...merged,
          ...(contextThread ? { thread: contextThread } : {}),
          ...(contextResource ? { resource: contextResource } : {}),
        },
      });
    } catch {
      return undefined;
    }
  }

  function reportLocalRecordingError(
    error: unknown,
    reason: MastraReplayReason,
    stage: StreamRecordingErrorStage,
    sessionId?: string,
  ): void {
    if (options.onRecordingError) {
      void Promise.resolve()
        .then(() =>
          options.onRecordingError?.({
            error,
            reason,
            stage,
            ...(sessionId === undefined ? {} : { sessionId }),
          }),
        )
        .catch(() => undefined);
    } else {
      console.warn(
        `Kitaru memory recording is unavailable for this turn (${reason})`,
      );
    }
  }

  /**
   * Store an ineligible session for a turn that ran natively because its
   * recording could not be set up, and close it once the native turn ends.
   */
  async function reportSetupFailure(
    error: unknown,
    reasonCode: MastraReplayReason,
    outcome: Promise<NativeOutcome>,
  ): Promise<void> {
    let sessionId: string | undefined;
    try {
      const session = await client.createSession({
        agent_id: options.agentId,
        agent_version_id: options.agentVersionId,
        adapter_version: (
          createRequire(import.meta.url)("../package.json") as {
            version: string;
          }
        ).version,
        framework: "mastra",
        inputs: {
          [MEMORY_REPLAY_KEY]: { version: 2, complete: false },
        },
        metadata: {
          mastra_replay_state: "ineligible",
          mastra_replay_reason: reasonCode,
          mastra_native_state: "started",
        },
        name: options.sessionName,
        origin: "recorded",
        outputs: null,
        started_at: new Date().toISOString(),
        status: "in_progress",
      });
      sessionId = session.id;
    } catch {
      // The report below still reaches the application without a session.
    }
    reportLocalRecordingError(error, reasonCode, "setup", sessionId);
    if (sessionId === undefined) return;
    const native = await outcome;
    const metadata = {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: reasonCode,
      mastra_native_state: native,
    };
    await client
      .updateSession(
        sessionId,
        native === "completed"
          ? {
              ended_at: new Date().toISOString(),
              metadata,
              status: "completed",
            }
          : {
              error: `KITARU_RECORDING_INCOMPLETE:${reasonCode}`,
              ended_at: new Date().toISOString(),
              metadata,
              status: "failed",
            },
      )
      .catch(() => undefined);
  }

  async function recordedStream(
    rawInput: unknown,
    callerOptions: RuntimeStreamOptions,
    downloads: FileDownloads,
    markRecordingStreamEntered: () => void,
  ): Promise<unknown> {
    assertMemoryReplayVersions();
    const { resolveModelConfig } = await import("@mastra/core/llm");
    const { MastraCompositeStore } = await import("@mastra/core/storage");
    const startedAt = new Date().toISOString();
    const invocationId = globalThis.crypto.randomUUID();
    let baselineFiles:
      | Awaited<ReturnType<typeof createCapturedFiles>>
      | undefined;
    const replay = await resolveReplayContext({
      allowedReplayModels: options.allowedReplayModels,
      callerInput: rawInput,
      client,
      recordedInputProjector: async (input) => {
        baselineFiles = await createCapturedFiles(
          typeof options.files === "function"
            ? await options.files({ input: rawInput, options: callerOptions })
            : (options.files ?? []),
          downloads.capture,
          supplied.fileCaptureWaitMs ?? DEFAULT_FILE_CAPTURE_WAIT_MS,
        );
        return encodeMemoryValue(baselineFiles.replaceDeclaredFileUrls(input));
      },
      requestedModelId: options.requestedModelId,
    });
    const historical = restoreMemoryReplayEnvelope(replay.effectiveInput);
    const invocationInput =
      historical?.rawInput ?? replay.effectiveRuntimeInput;
    if (Boolean(replay.spec) !== Boolean(historical))
      throw new Error(
        "Memory replay requires a complete recorded invocation and an active Kitaru replay.",
      );
    if (replay.override?.prompt != null)
      throw new Error(
        "Memory replay supports system_prompt overrides; replacing raw invocation input requires a new recording.",
      );
    const selector = historical?.initialSnapshot ?? getSelector(callerOptions);
    const liveContext = callerOptions.requestContext ?? new RequestContext();
    if (!historical) {
      try {
        validateMemoryReplaySelectors(
          selector,
          Object.fromEntries(liveContext.entries()),
        );
      } catch {
        throw new MemoryReplayContextError(
          "Request context contains incompatible memory selectors.",
        );
      }
    }
    const recordedContext = historical?.requestContext ?? {};
    const safeContext = requireRecord(
      decodeMemoryValue(encodeMemoryValue(recordedContext)),
      "request context",
    );
    try {
      validateMemoryReplayContext(selector, safeContext);
    } catch {
      throw new MemoryReplayContextError(
        "Request context is incompatible with recorded memory selectors.",
      );
    }
    const requestContext = historical
      ? new MemoryReplayRequestContext(selector)
      : liveContext;
    if (historical)
      for (const [key, value] of Object.entries(safeContext))
        requestContext.set(key, value);
    const abort = new AbortController();
    let state: AdapterRunState | undefined;
    let requestCapture: ReturnType<typeof createRequestCapture> | undefined;
    const getState = () => {
      if (!state) throw new Error("Memory recorder has not initialized.");
      return state;
    };
    const onIncomplete = (reason: string): void => {
      // Baseline diagnosis must not alter Mastra's native storage result.
      if (!historical || reason !== "Native memory storage mutation failed.")
        return;
      const error = new Error(reason);
      state?.storeFailure(error);
      abort.abort(error);
    };
    const writeNode = async (node: SessionNodeCreateRequest) => {
      const active = getState();
      await active.enqueueStep(async () => {
        await active.client.upsertSessionNodes(active.sessionId, {
          nodes: [node],
        });
      });
    };
    const recordMutation = async (event: MastraMemoryMutation) =>
      writeNode({
        external_id: event.id,
        parent_external_id: ROOT_NODE_EXTERNAL_ID,
        node_type: "span",
        name: "memory_mutation",
        status: event.complete ? "completed" : "failed",
        inputs: event.arguments,
        outputs: event.result,
        attributes: {
          invocation_id: event.invocationId,
          memory_revision: event.revision,
          memory_method: event.method,
          request_id: event.requestId ?? null,
          evidence_complete: event.complete && !event.truncationReasons,
          evidence_truncated: event.truncationReasons !== undefined,
          evidence_truncation_reasons: event.truncationReasons ?? [],
        },
      });
    const omCaptureErrors: string[] = [];
    const omTape = createOMResultTape(
      historical?.omTape as OMResultEntry[] | undefined,
      (reason) => omCaptureErrors.push(reason),
    );
    let replayMetadata: Record<string, JsonValue> | undefined;
    let tripwireListener: ((reason: string) => void) | undefined;
    let runtime: {
      memory: Memory;
      binding: MastraMemoryCaptureBinding;
      initialSnapshot: MastraMemorySnapshot | undefined;
      beginFinalization?(): void;
      /** Resolve false when memory work missed the finalization deadline. */
      finish(): Promise<boolean>;
      release(): Promise<void>;
    };
    let unsupportedEvidence = false;
    let markUnsupportedOnBinding: (() => void) | undefined;
    const markUnsupportedEvidence = () => {
      unsupportedEvidence = true;
      markUnsupportedOnBinding?.();
    };
    const sanitizer = historical
      ? createRecordedEvidenceSanitizer(new Map(), markUnsupportedEvidence)
      : baselineFiles?.evidenceSanitizer(markUnsupportedEvidence);
    if (!sanitizer)
      throw new Error("Controlled evidence sanitizer was not initialized.");
    let memoryStore: MastraMemoryStoreSemantics;
    if (historical) {
      memoryStore =
        historical.configuration.memoryStore === "in-memory"
          ? "in-memory"
          : "persistent";
      runtime = await createIsolatedMemoryReplay({
        invocationId,
        initialSnapshot: historical.initialSnapshot,
        storeSemantics: memoryStore,
        configuration: requireRecord(
          historical.configuration.memoryConfig,
          "memory configuration",
        ),
        resolveModel: options.resolveModel,
        recordMutation,
        onIncomplete,
        getRequestId: () => requestCapture?.currentRequestId,
        omTape,
        finalizationWaitMs,
      });
    } else {
      const source = await options.sourceMemory();
      memoryStore = await getMemoryStoreSemantics(source.domain);
      const binding = createMemoryCaptureBinding({
        invocationId,
        ...selector,
        domain: source.domain,
        exclusiveAccess: source.exclusiveAccess,
        sanitizeEvidence: sanitizer.replace,
        recordMutation,
        onIncomplete,
        getRequestId: () => requestCapture?.currentRequestId,
      });
      const { Memory } = await import("@mastra/memory");
      const memory = new Memory({
        storage: new MastraCompositeStore({
          id: `kitaru-baseline-${invocationId}`,
          domains: { memory: binding.domain },
        }),
        options: await bindOMResultModels(
          source.configuration,
          options.resolveModel,
          omTape,
        ),
      });
      const sourceEngine = source.memory
        ? await source.memory.omEngine.catch(() => null)
        : null;
      // The source settled() waits for work on every thread of that Memory.
      // Mastra tracks buffering per thread for the whole process, so join only
      // this thread's; the snapshot check rejects any other unjoined work.
      const initialSnapshot = await binding.captureInitial({
        settled: async () => {
          await (await memory.omEngine)?.waitForBuffering(
            selector.threadId,
            selector.resourceId,
            CAPTURE_BUFFERING_WAIT_MS,
          );
        },
      });
      let tracked = false;
      const trackSourceWork = () => {
        if (tracked || !sourceEngine) return;
        tracked = true;
        void sourceEngine
          .trackBackgroundWork(binding.settle(memory, finalizationWaitMs))
          .catch(() => undefined);
      };
      let finished: Promise<boolean> | undefined;
      runtime = {
        memory,
        binding,
        initialSnapshot,
        beginFinalization: trackSourceWork,
        finish() {
          finished ??= (async () => {
            trackSourceWork();
            const settled = await binding.settle(memory, finalizationWaitMs);
            if (!settled)
              binding.markIncomplete(
                "Observational-memory work did not settle before the finalization deadline.",
                "om_settle_timeout",
              );
            return settled;
          })();
          return finished;
        },
        release: () => binding.release(),
      };
    }
    markUnsupportedOnBinding = () =>
      runtime.binding.markIncomplete(
        "Recorded evidence contains an unsupported value.",
        "recorded_evidence_unsupported",
      );
    if (unsupportedEvidence) markUnsupportedOnBinding();
    try {
      const files = historical
        ? restoreCapturedFiles(historical.files)
        : baselineFiles;
      if (!files)
        throw new Error("Controlled file capture was not initialized.");
      const evidenceClient = recordingClient(client, sanitizer.replace, () =>
        unsupportedEvidence ? "recorded_evidence_unsupported" : undefined,
      );
      const workspace = options.skillsDirectory
        ? await loadSkillsWorkspace(
            options.skillsDirectory,
            historical?.configuration.workspaceManifest as Parameters<
              typeof loadSkillsWorkspace
            >[1],
          )
        : undefined;
      if (historical?.configuration.workspaceManifest && !workspace)
        throw new Error("Recorded skills workspace is missing.");
      const owned = bindMemoryToolIdentity(
        reportMemoryProcessorTripwires(runtime.memory, (reason) =>
          tripwireListener?.(reason),
        ),
      );
      const config = await factory({
        memory: owned.memory,
        resolveFile: files.resolveFile,
        workspace: workspace?.workspace,
      });
      if (config.memory !== undefined && config.memory !== owned.memory)
        unsupportedAgentConfiguration(
          "Agent factory must use its supplied Memory instance.",
        );
      if (
        config.workspace !== undefined &&
        config.workspace !== workspace?.workspace
      )
        unsupportedAgentConfiguration(
          "Agent factory must use its supplied pinned workspace.",
        );
      const dynamic = { requestContext, mastra: options.mastra };
      const instructions = historical
        ? historical.configuration.instructions
        : typeof config.instructions === "function"
          ? await config.instructions(dynamic)
          : config.instructions;
      const modelConfiguration = historical
        ? await options.resolveModel(
            replay.replacementModelId ??
              String(historical.configuration.modelId),
          )
        : typeof config.model === "function"
          ? await config.model(dynamic)
          : config.model;
      if (Array.isArray(modelConfiguration))
        throw new MastraReplayReasonError(
          "Model fallback arrays are outside memory replay support.",
          "model_identity_unsupported",
        );
      const nativeModel = await resolveModelConfig(
        modelConfiguration,
        requestContext,
      );
      const resolvedDefaults = historical
        ? historical.configuration.defaultOptions
        : typeof config.defaultOptions === "function"
          ? await config.defaultOptions(dynamic)
          : (config.defaultOptions ?? {});
      const defaults = requireRecord(resolvedDefaults, "default options");
      // Baseline resolvers receive the original mutable context. Only the
      // application's approved projection enters replay evidence.
      const projectedContext = historical
        ? Object.fromEntries(requestContext.entries())
        : (options.captureRequestContext?.(requestContext) ?? {});
      let effectiveContext: Record<string, unknown>;
      try {
        effectiveContext = requireRecord(
          decodeMemoryValue(encodeMemoryValue(projectedContext)),
          "request context",
        );
        validateMemoryReplayContext(selector, effectiveContext);
      } catch {
        throw new MemoryReplayContextError(
          "Request context cannot be captured for memory replay.",
        );
      }
      if (
        Object.keys(effectiveContext).some((key) =>
          /auth|credential|jwt|key|password|secret|token/i.test(key),
        )
      )
        throw new MemoryReplayContextError(
          "Unsupported replay request context credential key.",
          "credential_key_unsupported",
        );
      if (
        !historical &&
        !options.captureRequestContext &&
        [...requestContext.entries()].length > 0
      )
        runtime.binding.markIncomplete(
          "Request context was not captured safely.",
          "context_unsupported",
        );
      const { deepMerge } = await import("@mastra/core/utils");
      const callerData = { ...callerOptions };
      delete callerData.requestContext;
      delete callerData.abortSignal;
      for (const callback of ["onFinish", "onError", "onAbort", "onStepFinish"])
        delete callerData[callback];
      const effective = historical
        ? requireRecord(
            historical.configuration.runOptions,
            "invocation options",
          )
        : deepMerge(defaults, callerData);
      assertSupportedConfiguration(config, effective);
      const effectiveSelector = getSelector(effective);
      if (
        effectiveSelector.threadId !== selector.threadId ||
        effectiveSelector.resourceId !== selector.resourceId
      )
        throw new MemoryReplayContextError(
          "Invocation memory selectors differ from the captured selectors.",
        );
      if (record(effective.memory) && effective.memory.options !== undefined)
        throw new MastraReplayReasonError(
          "Per-call memory.options are unsupported. Set the complete memory configuration in sourceMemory instead.",
          "memory_config_unsupported",
        );
      const overrideSettings = parseModelSettings(
        replay.override?.model_params,
      );
      if (overrideSettings)
        effective.modelSettings = {
          ...(record(effective.modelSettings) ? effective.modelSettings : {}),
          ...overrideSettings,
        };
      const applicationInstructions =
        replay.override?.system_prompt ?? instructions;
      const configuration = {
        instructions: applicationInstructions,
        modelId: getMemoryModelId(modelConfiguration),
        defaultOptions: defaults,
        runOptions: effective,
        memoryConfig:
          historical?.configuration.memoryConfig ??
          serializeMemoryConfiguration(runtime.memory.getMergedThreadConfig()),
        memoryStore,
        ...(workspace ? { workspaceManifest: workspace.manifest } : {}),
      };
      let recordedRawInput = invocationInput;
      if (!historical) {
        if (!baselineFiles)
          throw new Error("Controlled file capture was not initialized.");
        recordedRawInput =
          baselineFiles.replaceDeclaredFileUrls(invocationInput);
      }
      // A replay's clock starts at the recorded turn's start, measured at the
      // same point before the stream as the baseline measured it.
      const turnStartedAt = historical ? historical.turnStartedAt : new Date();
      if (historical?.turnStartedAt)
        await pinMemoryClock(
          runtime.memory,
          createRecordedClock(historical.turnStartedAt),
        );
      // A failed capture already recorded why; the envelope repeats it
      // instead of reporting a missing snapshot.
      const captured = runtime.initialSnapshot
        ? captureMemoryReplayEnvelope(
            {
              invocationId,
              rawInput: recordedRawInput,
              initialSnapshot: runtime.initialSnapshot,
              configuration,
              requestContext: effectiveContext,
              files: files.files,
              turnStartedAt,
            },
            // Uploads pass through this sanitizer too; applying it first
            // keeps the recorded hash valid for the stored envelope.
            sanitizer.replace,
          )
        : {
            envelope: createIncompleteMemoryReplayEnvelope(
              runtime.binding.incompleteReasons.join(" ") ||
                "Initial memory was not captured.",
            ),
            reason: runtime.binding.incompleteReason,
          };
      const envelope = captured.envelope;
      if (!envelope.complete && historical)
        throw new Error(envelope.reasons.join(" "));
      const writeAttempt = (evidence: RequestEvidence, error: unknown) =>
        writeNode({
          external_id: evidence.externalId,
          parent_external_id: ROOT_NODE_EXTERNAL_ID,
          node_type: "llm_call",
          name: "model_request",
          status: "failed",
          error: describeProviderError(error) ?? "Model request failed",
          inputs: evidence.inputs,
          outputs: null,
          model: evidence.modelId,
          model_params: evidence.modelSettings,
          started_at: evidence.startedAt,
          ended_at: new Date().toISOString(),
          attributes: requestEvidenceAttributes(evidence),
        });
      const capture = createRequestCapture({
        invocationId,
        // Only limits the application chose bound request evidence; the
        // tool-sized defaults would truncate nearly every prompt.
        recordingLimits:
          supplied.recordingLimits === undefined
            ? undefined
            : options.recordingLimits,
        sanitizeEvidence: sanitizer.replace,
        getMemoryRevision: () => runtime.binding.revision,
        onFailedAttempt: writeAttempt,
        onCaptureError: () =>
          runtime.binding.markIncomplete(
            "Actor request evidence was incomplete.",
            "request_evidence_incomplete",
          ),
      });
      requestCapture = capture;
      const policy = createStatefulToolProcessors({
        tokens: owned.tokens,
        getState,
        sanitizeEvidence: sanitizer.replace,
        abort(reason) {
          state?.storeFailure(reason);
          abort.abort(reason);
        },
        adapter: options,
      });
      const contextAtCapture = new Map(requestContext.entries());
      const requestProcessor: InputProcessor = {
        id: "kitaru-effective-request",
        async processInputStep(args) {
          const currentContext =
            args.requestContext instanceof RequestContext
              ? args.requestContext
              : requestContext;
          const current = new Map(
            [...currentContext.entries()].filter(
              ([key]) => key !== "MastraMemory",
            ),
          );
          if (
            current.size !== contextAtCapture.size ||
            [...current].some(
              ([key, value]) => !Object.is(contextAtCapture.get(key), value),
            )
          ) {
            runtime.binding.markIncomplete(
              "Request context changed after replay capture.",
              "context_mutated_after_capture",
            );
            if (historical)
              throw new Error(
                "Unsupported Mastra memory replay: request context changed after capture.",
              );
          }
          capture.beginStep({
            stepNumber: args.stepNumber,
            messageList: args.messageList,
            applicationInstructions,
            extraContext: {
              system: effective.system ?? null,
              context: effective.context ?? [],
            },
          });
          const resolved = await resolveModelConfig(args.model, requestContext);
          return {
            model: capture.instrumentModel(resolved) as typeof args.model,
          };
        },
      };
      const agent = new Agent({
        ...config,
        memory: owned.memory,
        workspace: workspace?.workspace,
        instructions: applicationInstructions as AgentConfig["instructions"],
        model: capture.instrumentModel(nativeModel) as MastraModelConfig,
        defaultOptions: {},
        inputProcessors: [
          policy.first,
          ...((config.inputProcessors as InputProcessor[]) ?? []),
          policy.last,
          requestProcessor,
        ],
      });
      const runtimeOptions: RuntimeStreamOptions = {
        ...effective,
        onFinish: callerOptions.onFinish,
        onError: callerOptions.onError,
        onAbort: callerOptions.onAbort,
        onStepFinish: callerOptions.onStepFinish,
        // An empty adapter-created context changes Mastra's native stream lifecycle.
        ...(historical ||
        callerOptions.requestContext ||
        [...requestContext.entries()].length > 0
          ? { requestContext }
          : {}),
        abortSignal: callerOptions.abortSignal
          ? AbortSignal.any([callerOptions.abortSignal, abort.signal])
          : abort.signal,
      };
      const version = createRequire(import.meta.url)("../package.json") as {
        version: string;
      };
      markRecordingStreamEntered();
      return await streamWithRecording({
        adapterVersion: version.version,
        agent: agent as unknown as Parameters<
          typeof streamWithRecording
        >[0]["agent"],
        callerMessages: invocationInput,
        callerOptions: runtimeOptions,
        client: evidenceClient,
        options,
        replayInput: replay.effectiveInput,
        replay,
        nativeFallback: async (error, reason) => {
          try {
            await runtime.finish();
            await runtime.release();
          } catch (cleanupError) {
            reportLocalRecordingError(
              cleanupError,
              getReplayReason(cleanupError, "memory_lease_unavailable"),
              "setup",
            );
          }
          reportLocalRecordingError(error, reason, "setup");
          return runNativeBaseline(rawInput, callerOptions, downloads);
        },
        setupWaitMs: historical
          ? undefined
          : (supplied.sessionSetupWaitMs ?? DEFAULT_SESSION_SETUP_WAIT_MS),
        flushWaitMs: historical ? undefined : 2 * finalizationWaitMs,
        requestedModelId:
          replay.replacementModelId ?? String(configuration.modelId),
        sessionName: options.sessionName,
        startedAt,
        stateful: {
          input: { [MEMORY_REPLAY_KEY]: envelope },
          sanitizeEvidence: sanitizer.replace,
          initialize(value) {
            state = value;
          },
          takeRequest(failedStep) {
            const successful = capture.takeSuccessful();
            // Truncation to size bounds loses diagnostic detail only; replay
            // input comes from the envelope, not from request evidence.
            if (successful?.reasons.length)
              runtime.binding.markIncomplete(
                "Actor request evidence was incomplete.",
                "request_evidence_incomplete",
              );
            // A step that failed because its provider call threw reuses that
            // attempt's node, which already holds the request evidence.
            return (
              successful ?? (failedStep ? capture.takeFailed() : undefined)
            );
          },
          beginFinalization() {
            runtime.beginFinalization?.();
          },
          setTripwireListener(listener) {
            tripwireListener = listener;
          },
          getReplayMetadata: () => replayMetadata,
          async finish() {
            const settled = await runtime.finish();
            await capture.drain();
            if (!historical) await runtime.binding.verifyEligibility();
            // The turn's writes are settled and checked, so the next turn on
            // this thread can acquire while evidence uploads and the session
            // update are sent.
            await runtime.release();
            await runtime.binding.drain();
            // An OM call past the deadline may never return; the turn is
            // already ineligible, so do not wait for its tape entry.
            const tape = settled ? await omTape.finish() : undefined;
            const omResults = tape?.entries ?? [];
            for (const reason of omCaptureErrors)
              runtime.binding.markIncomplete(reason, "om_tape_incomplete");
            const divergence = historical ? tape?.divergence : undefined;
            if (divergence?.inputMismatches)
              await writeNode({
                external_id: `${invocationId}:om-input-mismatch`,
                parent_external_id: ROOT_NODE_EXTERNAL_ID,
                node_type: "span",
                name: "om_input_mismatch",
                status: "completed",
                inputs: null,
                outputs: null,
                attributes: { count: divergence.inputMismatches },
              });
            if (divergence?.surplusCalls || divergence?.unusedResults)
              await writeNode({
                external_id: `${invocationId}:om-call-divergence`,
                parent_external_id: ROOT_NODE_EXTERNAL_ID,
                node_type: "span",
                name: "om_call_divergence",
                status: "completed",
                inputs: null,
                outputs: null,
                attributes: {
                  surplus_calls: divergence.surplusCalls,
                  unused_results: divergence.unusedResults,
                },
              });
            if (divergence && Object.values(divergence).some(Boolean))
              replayMetadata = {
                mastra_om_divergence: {
                  input_mismatches: divergence.inputMismatches,
                  surplus_calls: divergence.surplusCalls,
                  unused_results: divergence.unusedResults,
                },
              };
            for (const pending of capture.flushUnfinished())
              await writeAttempt(
                pending,
                new Error("Unfinished model attempt"),
              );
            if (runtime.binding.incompleteReasons.length)
              throw new MastraReplayReasonError(
                runtime.binding.incompleteReasons.join(" "),
                runtime.binding.incompleteReason,
              );
            if (!envelope.complete)
              throw new MastraReplayReasonError(
                envelope.reasons.join(" "),
                captured.reason ?? "capture_prerequisite_failed",
              );
            return {
              [MEMORY_REPLAY_KEY]: finalizeMemoryReplayEnvelope(
                envelope,
                omResults.map((entry) => ({
                  phase: entry.phase,
                  ordinal: entry.ordinal,
                  method: entry.method,
                  inputFingerprint: entry.inputFingerprint,
                  output: entry.output,
                  ...(entry.failed ? { failed: true } : {}),
                })),
                sanitizer.replace,
              ),
            };
          },
          release: () => runtime.release(),
        },
      });
    } catch (error) {
      try {
        await runtime.finish();
        await runtime.release();
      } catch (cleanupError) {
        if (options.onRecordingError) {
          void Promise.resolve()
            .then(() =>
              options.onRecordingError?.({
                error: cleanupError,
                sessionId: state?.sessionId,
                stage: "complete",
              }),
            )
            .catch(() => undefined);
        } else {
          console.warn(
            "Kitaru memory cleanup failed after the invocation failed",
          );
        }
      }
      throw error;
    }
  }
  async function stream(
    rawInput: unknown,
    callerOptions: RuntimeStreamOptions = {},
  ): Promise<unknown> {
    let enteredRecordingStream = false;
    const downloads = createFileDownloads(
      options.resolveFile ??
        (async () => {
          throw new Error("Missing controlled file resolver.");
        }),
    );
    try {
      return await recordedStream(rawInput, callerOptions, downloads, () => {
        enteredRecordingStream = true;
      });
    } catch (error) {
      if (
        enteredRecordingStream ||
        process.env.KITARU_REPLAY_ID ||
        process.env.KITARU_TASK_INPUTS ||
        process.env.KITARU_OVERRIDE
      )
        throw error;
      const reasonCode =
        error instanceof FileCaptureTimeoutError
          ? "file_capture_timeout"
          : getReplayReason(error, "capture_setup_failed");
      const native = await runNativeBaseline(
        rawInput,
        callerOptions,
        downloads,
      );
      void reportSetupFailure(error, reasonCode, native.outcome);
      return native.result;
    }
  }
  return { stream: stream as Agent["stream"] };
}
