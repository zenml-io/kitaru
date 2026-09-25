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
  type RecordingLimits,
  ROOT_NODE_EXTERNAL_ID,
  resolveReplayContext,
} from "@zenml-io/kitaru/adapter";
import {
  recordAttachmentTokens,
  replayAttachmentTokens,
} from "./attachment-tokens.js";
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
  isAsyncBufferingRunning,
  type MastraMemoryStoreSemantics,
  pinMemoryClock,
  serializeMemoryConfiguration,
} from "./memory-replay.js";
import {
  captureMemoryReplayEnvelope,
  createIncompleteMemoryReplayEnvelope,
  decodeMemoryValue,
  encodeMemoryEvidence,
  encodeMemoryValue,
  finalizeMemoryReplayEnvelope,
  type MastraMemorySnapshot,
  type MastraRecordedFile,
  MEMORY_REPLAY_KEY,
  restoreMemoryReplayEnvelope,
  validateMemoryReplayContext,
  validateMemoryReplaySelectors,
} from "./memory-snapshot.js";
import {
  createOMResultTape,
  MastraOMDivergenceError,
  type MissingOMResults,
  type OMLiveCall,
  type OMResultEntry,
} from "./om-result-tape.js";
import { describeProviderError } from "./provider-errors.js";
import { createRecordedClock } from "./replay-clock.js";
import { assertStableToolName } from "./replay-guards.js";
import {
  createIneligibleMetadata,
  describeReplayFailure,
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
  collectFileNetworkUrls,
  containsModelFileUrl,
  createCapturedContentReferencer,
  createCapturedFiles,
  createFileBlobStore,
  createFileDownloads,
  createInlineContentReferencer,
  createInlineFileReader,
  createRecordedEvidenceSanitizer,
  createThreadFileRegistry,
  FileCaptureTimeoutError,
  loadRecordedFiles,
  referenceInlineFiles,
  referenceInputFilesAsUrls,
  restoreCapturedFiles,
} from "./stateful-files.js";
import {
  bindMemoryToolIdentity,
  createStatefulToolProcessors,
  reportMemoryProcessorTripwires,
  reportProcessorTripwires,
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
  /**
   * What a replay does when observational memory needs a blocking observer or
   * reflector result that the baseline never recorded, such as after the
   * replayed actor takes a step production never took.
   *
   * `fail` (the default) ends the replay as diverged with
   * `mastra_om_call_order`. `live` calls the observer or reflector model,
   * resolved with `resolveModel` from its recorded identity, for those calls
   * only. Recorded results still answer every other OM call. Each live call is
   * recorded as an `llm_call` node, and the replay session reports
   * `mastra_om_live_calls` in its metadata.
   */
  missingObservationalMemoryResults?: MissingOMResults;
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

/**
 * Serve declared URLs from captured bytes, and fetch any other URL as a native
 * turn would.
 *
 * A URL from the turn's input or thread history is captured as it is fetched,
 * so replay serves its bytes. Any other URL makes the turn ineligible, because
 * replay could not fetch it.
 */
function resolveBaselineFile(
  captured: Awaited<ReturnType<typeof createCapturedFiles>>,
  downloads: FileDownloads,
  markIncomplete: (message: string, reason: MastraReplayReason) => void,
): MemoryReplayAgentBindings["resolveFile"] {
  return async (url) => {
    if (captured.isDeclared(url)) return captured.resolveFile(url);
    if (captured.isConversationUrl(url)) {
      let file: Awaited<ReturnType<MemoryReplayAgentBindings["resolveFile"]>>;
      try {
        file = await downloads.resolveNative(url);
      } catch (error) {
        markIncomplete(
          "An input or thread history file failed to download.",
          "file_capture_failed",
        );
        throw error;
      }
      try {
        captured.recordConversationFile(url, file);
      } catch (error) {
        markIncomplete(
          describeReplayFailure(
            error,
            "An input or thread history file was invalid.",
          ),
          getReplayReason(error, "file_capture_failed"),
        );
      }
      return file;
    }
    markIncomplete(
      "The agent resolved a file URL that was not declared in files.",
      "file_url_undeclared",
    );
    return downloads.resolveNative(url);
  };
}

export type MemoryReplayAgentFactory = (
  bindings: MemoryReplayAgentBindings,
) => AgentConfig | Promise<AgentConfig>;

const DEFAULT_FINALIZATION_WAIT_MS = 60_000;
const DEFAULT_SESSION_SETUP_WAIT_MS = 2_000;
const DEFAULT_FILE_CAPTURE_WAIT_MS = 10_000;

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
  /** Report a processor tripwire, which ends the turn without a callback. */
  reportTripwire(): void;
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
    reportTripwire: () => settle("failed"),
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

/**
 * Build the `llm_call` node for an OM call that a replay answered live.
 *
 * The prompt keeps captured files as their references, so the node does not
 * repeat file bytes the replay input already names.
 */
function liveOMCallNode(
  invocationId: string,
  index: number,
  call: OMLiveCall,
  sanitize: (value: unknown) => unknown,
  limits: RecordingLimits | undefined,
): SessionNodeCreateRequest {
  const lossReasons: string[] = [];
  const encode = (value: unknown, label: string): JsonValue => {
    try {
      const evidence = encodeMemoryEvidence(sanitize(value), label, limits);
      if (evidence.lossReason) lossReasons.push(evidence.lossReason);
      return evidence.value;
    } catch {
      lossReasons.push(`${label} could not be recorded.`);
      return null;
    }
  };
  // The tape keeps the result in codec form; encoding that form again would
  // reject its own reserved keys.
  let output: unknown = null;
  try {
    output = decodeMemoryValue(call.output);
  } catch {
    lossReasons.push("Live observational-memory result could not be recorded.");
  }
  let model: string | null = null;
  try {
    model = getMemoryModelId(call.model);
  } catch {
    // The node still shows the call; only its model name is unknown.
  }
  return {
    external_id: `${invocationId}:om-live-call:${index}`,
    parent_external_id: ROOT_NODE_EXTERNAL_ID,
    node_type: "llm_call",
    name: `om_${call.phase}_live_call`,
    status: call.failed ? "failed" : "completed",
    ...(call.failed ? { error: "Observational-memory model call failed" } : {}),
    inputs: encode(
      { prompt: call.prompt },
      "Live observational-memory request",
    ),
    outputs: encode(output, "Live observational-memory result"),
    model,
    started_at: call.startedAt,
    ended_at: call.endedAt,
    attributes: {
      invocation_id: invocationId,
      om_phase: call.phase,
      om_method: call.method,
      om_live: true,
      evidence_complete: call.captured && lossReasons.length === 0,
      evidence_loss_reasons: lossReasons,
    },
  };
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
  // Shared by every turn so a file already stored is not uploaded again.
  const fileBlobs = createFileBlobStore(client.blobs);
  const threadFiles = createThreadFileRegistry();
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
    const observed = observeNativeOutcome(callerOptions);
    const native = new Agent({
      ...config,
      memory: reportMemoryProcessorTripwires(memory, observed.reportTripwire),
      ...(Array.isArray(config.inputProcessors)
        ? {
            inputProcessors: config.inputProcessors.map((processor) =>
              reportProcessorTripwires(processor, observed.reportTripwire),
            ),
          }
        : {}),
    }) as unknown as {
      stream(input: unknown, options: RuntimeStreamOptions): Promise<unknown>;
    };
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
        metadata: createIneligibleMetadata(reasonCode, "started"),
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
    await client
      .updateSession(sessionId, {
        ended_at: new Date().toISOString(),
        metadata: createIneligibleMetadata(reasonCode, native),
        ...(native === "completed"
          ? { status: "completed" as const }
          : {
              error: `KITARU_RECORDING_INCOMPLETE:${reasonCode}`,
              status: "failed" as const,
            }),
      })
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
        // Input files are captured when a processor resolves them, like
        // history files; without the application's resolver none can be.
        if (options.resolveFile)
          baselineFiles.acceptConversationUrls(collectFileNetworkUrls(input));
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
    let replayMetadata: Record<string, JsonValue> | undefined;
    let tripwireListener: ((reason: string) => Promise<void>) | undefined;
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
    const omCaptureErrors: string[] = [];
    let omEngine: Awaited<Memory["omEngine"]> = null;
    const replayFiles = historical
      ? restoreCapturedFiles(
          await loadRecordedFiles(historical.files, client.blobs),
        )
      : undefined;
    const files = replayFiles ?? baselineFiles;
    if (!files) throw new Error("Controlled file capture was not initialized.");
    const readInlineFile = createInlineFileReader();
    // A stored message can hold a file captured on an earlier turn of this
    // thread inline, where the application's processor wrote its bytes.
    const isKnownFile = (reference: string): boolean =>
      files.hasFile(reference) ||
      (!historical && threadFiles.has(selector, reference));
    const referenceKnownContent = createCapturedContentReferencer(
      isKnownFile,
      readInlineFile,
    );
    const referenceInitialContent = createInlineContentReferencer(
      readInlineFile,
      isKnownFile,
    );
    const omTape = createOMResultTape(
      historical?.omTape as OMResultEntry[] | undefined,
      (reason) => omCaptureErrors.push(reason),
      {
        missingResults: supplied.missingObservationalMemoryResults ?? "fail",
        resolveFileReference: async (reference) => {
          if (!replayFiles) throw new Error("Recorded files were not loaded.");
          return replayFiles.resolveFile(reference);
        },
        mapString: (value) => sanitizer.replace(value),
        readInlineFile,
        isCapturedFile: baselineFiles
          ? (reference) => baselineFiles?.hasFile(reference) ?? false
          : undefined,
        isBuffered: (phase) =>
          omEngine !== null &&
          isAsyncBufferingRunning(
            omEngine,
            selector,
            phase === "observer" ? "observation" : "reflection",
          ),
      },
    );
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
        readFile: replayFiles?.readFile,
        referenceFileContent: referenceKnownContent,
        referenceInitialContent,
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
        referenceFileContent: referenceKnownContent,
        referenceInitialContent,
        // Without the application's resolver, no history file can be
        // captured, so history URLs stay undeclared.
        acceptHistoryFileUrls: options.resolveFile
          ? (urls) => baselineFiles?.acceptConversationUrls(urls)
          : undefined,
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
      // Buffering an earlier turn left running on this thread would change
      // the snapshot after capture. Waiting for it would delay the native
      // answer, so the turn is ineligible at once instead.
      const initialSnapshot = await binding.captureInitial({
        settled: async () => {
          const engine = await memory.omEngine;
          if (
            engine &&
            (isAsyncBufferingRunning(engine, selector, "observation") ||
              isAsyncBufferingRunning(engine, selector, "reflection"))
          )
            throw new MastraReplayReasonError(
              "Observational-memory buffering from an earlier turn is still running.",
              "om_work_unjoined",
            );
        },
      });
      let tracked = false;
      const trackSourceWork = () => {
        if (tracked) return;
        tracked = true;
        void binding.beginFinalization();
        if (!sourceEngine) return;
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
    omEngine = await runtime.memory.omEngine;
    const attachmentTokens =
      omEngine && !historical
        ? recordAttachmentTokens(omEngine, (value) => {
            // A history URL the turn never resolved stays in the recorded
            // history with its credentials redacted, so its count is kept
            // under that form.
            const key = sanitizer.replace(value);
            return /^(?:kitaru-file|https?):\/\//i.test(key) ? key : undefined;
          })
        : undefined;
    if (omEngine && historical)
      replayAttachmentTokens(omEngine, historical.attachmentTokens ?? {});
    markUnsupportedOnBinding = () =>
      runtime.binding.markIncomplete(
        "Recorded evidence contains an unsupported value.",
        "recorded_evidence_unsupported",
      );
    if (unsupportedEvidence) markUnsupportedOnBinding();
    try {
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
        resolveFile:
          historical || !baselineFiles
            ? files.resolveFile
            : resolveBaselineFile(baselineFiles, downloads, (message, reason) =>
                runtime.binding.markIncomplete(message, reason),
              ),
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
      const referenceSnapshot = (
        initialSnapshot: MastraMemorySnapshot,
        recordedFiles: readonly MastraRecordedFile[],
      ) =>
        referenceInlineFiles(initialSnapshot, {
          read: readInlineFile,
          isKnown: isKnownFile,
          files: recordedFiles,
        });
      const captureEnvelope = (
        initialSnapshot: MastraMemorySnapshot,
        recordedFiles: MastraRecordedFile[],
      ) =>
        captureMemoryReplayEnvelope(
          {
            invocationId,
            rawInput: recordedRawInput,
            initialSnapshot,
            configuration,
            requestContext: effectiveContext,
            files: recordedFiles,
            turnStartedAt,
          },
          // Uploads pass through this sanitizer too; applying it first
          // keeps the recorded hash valid for the stored envelope.
          sanitizer.replace,
        );
      // A failed capture already recorded why; the envelope repeats it
      // instead of reporting a missing snapshot.
      const initialFiles = files.files;
      const initial = runtime.initialSnapshot
        ? referenceSnapshot(runtime.initialSnapshot, initialFiles)
        : undefined;
      const captured = initial
        ? captureEnvelope(initial.snapshot, [...initialFiles, ...initial.files])
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
        sanitizeEvidence: (value) =>
          sanitizer.replace(referenceKnownContent(value)),
        getMemoryRevision: () => runtime.binding.revision,
        onFailedAttempt: writeAttempt,
        onCaptureError: () =>
          runtime.binding.markIncomplete(
            "Actor request evidence was incomplete.",
            "request_evidence_incomplete",
          ),
      });
      requestCapture = capture;
      // Only limits the application chose bound tool payloads, as for
      // request evidence.
      const toolRecordingLimits =
        supplied.recordingLimits === undefined
          ? undefined
          : options.recordingLimits;
      const policy = createStatefulToolProcessors({
        tokens: owned.tokens,
        getState,
        recordingLimits: toolRecordingLimits,
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
          if (containsModelFileUrl(args.messageList.get.all.db())) {
            const message =
              "A file part reached the model as a URL instead of its content. A processor must replace it with the bytes from resolveFile.";
            runtime.binding.markIncomplete(message, "file_url_sent_to_model");
            // Mastra or the provider would fetch the URL, or fail on a
            // captured file reference, outside the recorded files.
            if (historical)
              throw new MastraReplayReasonError(
                `Unsupported Mastra memory replay: ${message}`,
                "file_url_sent_to_model",
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
          ...((config.inputProcessors as InputProcessor[]) ?? []).map(
            (processor) =>
              reportProcessorTripwires(processor, (reason) =>
                tripwireListener?.(reason),
              ),
          ),
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
        callerMessages: historical
          ? referenceInputFilesAsUrls(invocationInput)
          : invocationInput,
        callerOptions: runtimeOptions,
        client: evidenceClient,
        options,
        replayInput: replay.effectiveInput,
        replay,
        nativeFallback: async (error, reason) => {
          try {
            // The turn's Memory has not run, so there is no work of its own
            // to join; joining would wait for other turns' buffering.
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
          recordingLimits: toolRecordingLimits,
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
            if (!historical) await runtime.binding.verifyEligibility();
            // The turn's writes are settled and checked, so the next turn on
            // this thread can acquire while evidence uploads, including failed
            // provider attempts, and the session update are sent.
            await runtime.release();
            await capture.drain();
            await runtime.binding.drain();
            // An OM call past the deadline may never return; the turn is
            // already ineligible, so do not wait for its tape entry.
            const tape = settled
              ? await omTape.finish().catch(async (error: unknown) => {
                  // The session error is a fixed code, so this node is
                  // where the failed replay names the call that diverged.
                  if (error instanceof MastraOMDivergenceError && error.call)
                    await writeNode({
                      external_id: `${invocationId}:om-unanswered-call`,
                      parent_external_id: ROOT_NODE_EXTERNAL_ID,
                      node_type: "span",
                      name: "om_unanswered_call",
                      status: "failed",
                      inputs: null,
                      outputs: null,
                      error: error.message,
                      attributes: {
                        phase: error.call.phase,
                        method: error.call.method,
                        replay_call: error.call.replayCall,
                        cause: error.call.cause,
                      },
                    });
                  throw error;
                })
              : undefined;
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
                attributes: {
                  count: divergence.inputMismatches,
                  calls: (tape?.inputMismatches ?? []).map((call) => ({
                    phase: call.phase,
                    method: call.method,
                    recorded_ordinal: call.recordedOrdinal,
                    replay_call: call.replayCall,
                  })),
                },
              });
            for (const [index, call] of (tape?.liveCalls ?? []).entries())
              await writeNode(
                liveOMCallNode(
                  invocationId,
                  index,
                  call,
                  sanitizer.replace,
                  toolRecordingLimits,
                ),
              );
            if (
              divergence?.surplusCalls ||
              divergence?.unusedResults ||
              divergence?.liveCalls
            )
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
                  live_calls: divergence.liveCalls,
                },
              });
            if (divergence && Object.values(divergence).some(Boolean))
              replayMetadata = {
                mastra_om_divergence: {
                  input_mismatches: divergence.inputMismatches,
                  surplus_calls: divergence.surplusCalls,
                  unused_results: divergence.unusedResults,
                  live_calls: divergence.liveCalls,
                },
                // A replay whose observer or reflector ran live no longer
                // reuses only what production observed.
                ...(divergence.liveCalls
                  ? { mastra_om_live_calls: divergence.liveCalls }
                  : {}),
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
            // Input and history files the turn resolved were captured after
            // the envelope was built, and every captured file is stored as a
            // blob only now, so the envelope is built again with their
            // references and blob ids.
            const initialSnapshot = historical
              ? undefined
              : (runtime.binding.sanitizeInitialAgain() ??
                runtime.initialSnapshot);
            let recaptured = captured;
            if (initialSnapshot) {
              // Input files the turn resolved were captured after the input
              // was projected, so it is projected again with their references.
              if (baselineFiles)
                recordedRawInput =
                  baselineFiles.replaceDeclaredFileUrls(invocationInput);
              const turnFiles = files.files;
              const referenced = referenceSnapshot(initialSnapshot, turnFiles);
              const storedFiles = await fileBlobs.store([
                ...turnFiles,
                ...referenced.files,
              ]);
              threadFiles.remember(
                selector,
                storedFiles.map((file) => file.url),
              );
              recaptured = captureEnvelope(referenced.snapshot, storedFiles);
            }
            if (!recaptured.envelope.complete)
              throw new MastraReplayReasonError(
                recaptured.envelope.reasons.join(" "),
                recaptured.reason ?? "capture_prerequisite_failed",
              );
            return {
              [MEMORY_REPLAY_KEY]: finalizeMemoryReplayEnvelope(
                recaptured.envelope,
                omResults.map((entry) => ({
                  phase: entry.phase,
                  ordinal: entry.ordinal,
                  method: entry.method,
                  inputFingerprint: entry.inputFingerprint,
                  output: entry.output,
                  ...(entry.failed ? { failed: true } : {}),
                })),
                sanitizer.replace,
                attachmentTokens?.counts(),
                // A replay's own input keeps files recorded inline before
                // blob storage unstored; no replay starts from it.
                Boolean(historical),
              ),
            };
          },
          release: () => runtime.release(),
        },
      });
    } catch (error) {
      try {
        // Joining would hold a baseline's native answer or error behind other
        // turns' buffering. A late write of this Memory still registers and
        // makes an overlapping turn ineligible.
        if (historical) await runtime.finish();
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
