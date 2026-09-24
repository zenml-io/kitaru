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
  serializeMemoryConfiguration,
} from "./memory-replay.js";
import {
  createMemoryReplayEnvelope,
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
import { assertStableToolName } from "./replay-guards.js";
import {
  createRequestCapture,
  type RequestEvidence,
  requestEvidenceAttributes,
} from "./request-capture.js";
import {
  createCapturedFiles,
  createRecordedEvidenceSanitizer,
  restoreCapturedFiles,
  type UnsafeEvidenceReason,
} from "./stateful-files.js";
import {
  bindMemoryToolIdentity,
  createStatefulToolProcessors,
  reportMemoryProcessorTripwires,
} from "./stateful-tools.js";
import { loadSkillsWorkspace } from "./stateful-workspace.js";
import {
  StatefulRecordingError,
  streamWithRecording,
} from "./stream-recording.js";
import type { KitaruAgentOptions, RuntimeStreamOptions } from "./types.js";

interface MastraMemorySource {
  settled(): Promise<void>;
  /**
   * The source Memory instance. When supplied, its `settled()` also joins the
   * memory work each recorded turn runs on its own Memory instance, and Kitaru
   * joins only the recorded thread's buffered work before capturing a turn.
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
    createSession: (request) => client.createSession(sanitize(request)),
    getReplay: client.getReplay.bind(client),
    getTaskSpec: client.getTaskSpec.bind(client),
    lookupToolResult: client.lookupToolResult.bind(client),
    upsertSessionNodes: (sessionId, request) =>
      client.upsertSessionNodes(sessionId, sanitize(request)),
    updateSession: (sessionId, request) => {
      const safe = sanitize(request);
      const reason = unsafeReason();
      if (reason && safe.metadata?.mastra_replay_state === "eligible") {
        safe.metadata = {
          ...safe.metadata,
          mastra_replay_state: "ineligible",
          mastra_replay_reason: reason,
        };
      }
      return client.updateSession(sessionId, safe);
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
  files?: readonly string[];
  resolveFile?: (
    url: string,
  ) => Promise<{ bytes: Uint8Array; mediaType: string }>;
  skillsDirectory?: string;
  resolveModel: (id: string) => MastraModelConfig | Promise<MastraModelConfig>;
  /**
   * How long a turn waits after its stream closes for observational-memory
   * work. A baseline then releases the source lease, and a turn whose work did
   * not settle in time is recorded as ineligible; a replay whose work did not
   * settle fails. Defaults to 60 seconds.
   */
  finalizationWaitMs?: number;
}

export interface MemoryReplayAgentBindings {
  memory: Memory;
  resolveFile(url: string): Promise<{ bytes: Uint8Array; mediaType: string }>;
  workspace?: Awaited<ReturnType<typeof loadSkillsWorkspace>>["workspace"];
}

export type MemoryReplayAgentFactory = (
  bindings: MemoryReplayAgentBindings,
) => AgentConfig | Promise<AgentConfig>;

const DEFAULT_FINALIZATION_WAIT_MS = 60_000;
const CAPTURE_BUFFERING_WAIT_MS = 5_000;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!record(value))
    throw new Error(`Unsupported Mastra memory replay: missing ${label}.`);
  return value;
}
class MemoryReplayContextError extends Error {}

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
      throw new Error(`Unsupported memory replay configuration '${name}'.`);
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
      throw new Error(`Unsupported serialized memory replay option '${name}'.`);
  }
  for (const name of Object.keys(config.tools ?? {}))
    assertStableToolName(name);
  for (const name of ["experimental_sandbox", "delegation", "backgroundTasks"])
    if (options[name] !== undefined)
      throw new Error(`Unsupported memory replay option ${name}.`);
  if (
    typeof config.tools === "function" ||
    typeof config.inputProcessors === "function" ||
    typeof config.workspace === "function"
  )
    throw new Error(
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
    throw new Error(
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
  ): Promise<unknown> {
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
      (reason) => reportLocalRecordingError(new Error(reason)),
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
      resolveFile:
        options.resolveFile ??
        (async () => {
          throw new Error("Missing controlled file resolver.");
        }),
      workspace: workspace?.workspace,
    });
    selector = await getNativeSelector(config, callerOptions);
    const native = new Agent({ ...config, memory }) as unknown as {
      stream(input: unknown, options: RuntimeStreamOptions): Promise<unknown>;
    };
    return native.stream(rawInput, callerOptions);
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

  function reportLocalRecordingError(error: unknown): void {
    if (options.onRecordingError) {
      void Promise.resolve()
        .then(() => options.onRecordingError?.({ error, stage: "complete" }))
        .catch(() => undefined);
    } else {
      console.warn("Kitaru memory recording is unavailable for this turn");
    }
  }

  async function reportSetupFailure(
    error: unknown,
    reasonCode: string,
  ): Promise<void> {
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
      await client.updateSession(session.id, {
        error: `KITARU_RECORDING_INCOMPLETE:${reasonCode}`,
        ended_at: new Date().toISOString(),
        status: "failed",
      });
    } catch {
      reportLocalRecordingError(error);
    }
  }

  async function recordedStream(
    rawInput: unknown,
    callerOptions: RuntimeStreamOptions = {},
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
          options.files ?? [],
          options.resolveFile ??
            (async () => {
              throw new Error("Missing controlled file resolver.");
            }),
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
          evidence_complete: event.complete,
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
    let unsafeEvidenceReason: UnsafeEvidenceReason | undefined;
    let markUnknownOnBinding: (() => void) | undefined;
    const markUnknownCredentialUrl = (reason: UnsafeEvidenceReason) => {
      if (!unsafeEvidenceReason || reason === "credential_url")
        unsafeEvidenceReason = reason;
      markUnknownOnBinding?.();
    };
    const sanitizer = historical
      ? createRecordedEvidenceSanitizer(new Map(), markUnknownCredentialUrl)
      : baselineFiles?.evidenceSanitizer(markUnknownCredentialUrl);
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
      // Once the source Memory joins every turn's work, its settled() also
      // waits for turns on other threads. Join only this thread's buffering.
      const initialSnapshot = await binding.captureInitial(
        source.memory
          ? {
              settled: async () => {
                await (await memory.omEngine)?.waitForBuffering(
                  selector.threadId,
                  selector.resourceId,
                  CAPTURE_BUFFERING_WAIT_MS,
                );
              },
            }
          : source,
      );
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
              );
            return settled;
          })();
          return finished;
        },
        release: () => binding.release(),
      };
    }
    markUnknownOnBinding = () =>
      runtime.binding.markIncomplete(
        "Recorded evidence contains an uncaptured credential URL or unsupported value.",
      );
    if (unsafeEvidenceReason) markUnknownOnBinding();
    try {
      const files = historical
        ? restoreCapturedFiles(historical.files)
        : baselineFiles;
      if (!files)
        throw new Error("Controlled file capture was not initialized.");
      const evidenceClient = recordingClient(client, sanitizer.replace, () =>
        unsafeEvidenceReason === "credential_url"
          ? "credential_url_uncaptured"
          : unsafeEvidenceReason === "unsupported_value"
            ? "recorded_evidence_unsupported"
            : undefined,
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
        throw new Error("Agent factory must use its supplied Memory instance.");
      if (
        config.workspace !== undefined &&
        config.workspace !== workspace?.workspace
      )
        throw new Error(
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
        throw new Error(
          "Model fallback arrays are outside memory replay support.",
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
        );
      if (
        !historical &&
        !options.captureRequestContext &&
        [...requestContext.entries()].length > 0
      )
        runtime.binding.markIncomplete(
          "Request context was not captured safely.",
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
        throw new Error(
          "Per-call memory.options are unsupported. Set the complete memory configuration in sourceMemory instead.",
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
      const envelope = createMemoryReplayEnvelope({
        invocationId,
        rawInput: recordedRawInput,
        initialSnapshot: runtime.initialSnapshot as MastraMemorySnapshot,
        configuration,
        requestContext: effectiveContext,
        files: files.files,
      });
      if (!envelope.complete && historical)
        throw new Error(envelope.reasons.join(" "));
      const writeAttempt = (evidence: RequestEvidence, error: unknown) =>
        writeNode({
          external_id: evidence.externalId,
          parent_external_id: ROOT_NODE_EXTERNAL_ID,
          node_type: "llm_call",
          name: "model_request",
          status: "failed",
          error: error instanceof Error ? error.name : "Model request failed",
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
        sanitizeEvidence: sanitizer.replace,
        getMemoryRevision: () => runtime.binding.revision,
        onFailedAttempt: writeAttempt,
        onCaptureError: () =>
          runtime.binding.markIncomplete(
            "Actor request evidence was incomplete.",
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
        nativeFallback: async (error) => {
          try {
            await runtime.finish();
            await runtime.release();
          } catch (cleanupError) {
            reportLocalRecordingError(cleanupError);
          }
          reportLocalRecordingError(error);
          return runNativeBaseline(rawInput, callerOptions);
        },
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
          takeRequest() {
            const evidence = capture.takeSuccessful();
            if (evidence && !evidence.complete)
              runtime.binding.markIncomplete(
                "Actor request evidence was incomplete.",
              );
            return evidence;
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
              runtime.binding.markIncomplete(reason);
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
            if (runtime.binding.incompleteReasons.length) {
              const message = runtime.binding.incompleteReasons.join(" ");
              const reasonCode = runtime.binding.incompleteReasons.includes(
                "Native memory storage mutation failed.",
              )
                ? "memory_mutation_failed"
                : runtime.binding.incompleteReasons.includes(
                      "Request context changed after replay capture.",
                    )
                  ? "context_mutated_after_capture"
                  : "memory_evidence_incomplete";
              throw new StatefulRecordingError(message, reasonCode);
            }
            if (!envelope.complete)
              throw new StatefulRecordingError(
                envelope.reasons.join(" "),
                "capture_prerequisite_failed",
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
    try {
      return await recordedStream(rawInput, callerOptions, () => {
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
        error instanceof Error &&
        /requires @mastra\/(?:core|memory)@/.test(error.message)
          ? "version_mismatch"
          : error instanceof MemoryReplayContextError
            ? "context_unsupported"
            : "capture_setup_failed";
      const nativeResult = await runNativeBaseline(rawInput, callerOptions);
      void reportSetupFailure(error, reasonCode);
      return nativeResult;
    }
  }
  return { stream: stream as Agent["stream"] };
}
