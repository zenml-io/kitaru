import { createRequire } from "node:module";
import type { Agent } from "@mastra/core/agent";
import type { MastraModelConfig } from "@mastra/core/llm";
import { KitaruClient } from "@zenml-io/kitaru";
import {
  boundedRecorderJson,
  parseModelSettings,
  RunRecorder,
  resolveReplayContext,
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
import { prepareStructuredOutputModel } from "./structured-output-model.js";
import { createToolHooks } from "./tool-policies.js";
import type {
  GenerateCapable,
  GenerateMethod,
  KitaruAgentOptions,
  RuntimeGenerateOptions,
} from "./types.js";

const packageMetadata: unknown = createRequire(import.meta.url)(
  "../package.json",
);
if (
  typeof packageMetadata !== "object" ||
  packageMetadata === null ||
  !("version" in packageMetadata) ||
  typeof packageMetadata.version !== "string"
) {
  throw new TypeError("The package manifest must contain a string version");
}
const ADAPTER_VERSION = packageMetadata.version;

type RuntimeGenerate = (
  messages: unknown,
  options?: RuntimeGenerateOptions,
) => Promise<unknown>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readableModelId(model: unknown): string | undefined {
  if (typeof model === "string") {
    return model;
  }
  if (!isRecord(model)) {
    return undefined;
  }
  if (typeof model.modelId === "string") {
    return model.modelId;
  }
  if (typeof model.id === "string") {
    return model.id;
  }
  return undefined;
}

function structuredOutputUsesModel(options: RuntimeGenerateOptions): boolean {
  return (
    isRecord(options.structuredOutput) &&
    Object.hasOwn(options.structuredOutput, "model") &&
    options.structuredOutput.model !== undefined &&
    options.structuredOutput.model !== null
  );
}

function tripwireReason(value: unknown): string | undefined {
  if (!isRecord(value) || !isRecord(value.tripwire)) {
    return undefined;
  }
  return typeof value.tripwire.reason === "string" &&
    value.tripwire.reason.length > 0
    ? value.tripwire.reason
    : "Mastra processor tripwire triggered";
}

export class KitaruAgent<TAgent extends GenerateCapable> {
  readonly generate: GenerateMethod<TAgent>;

  readonly #agent: TAgent;
  readonly #client: KitaruClient;
  readonly #options: KitaruAgentOptions;
  readonly #sessionName?: string;

  constructor(agent: TAgent, options: KitaruAgentOptions) {
    this.#agent = agent;
    this.#options = options;
    this.#sessionName = options.sessionName ?? process.env.KITARU_SESSION_NAME;
    this.#client = new KitaruClient({
      apiKey: options.apiKey,
      apiUrl: options.apiUrl,
      timeoutMs: options.timeoutMs,
    });
    this.generate = this.#generate.bind(this) as GenerateMethod<TAgent>;
  }

  async #generate(
    callerMessages: unknown,
    callerOptions: RuntimeGenerateOptions = {},
  ): Promise<unknown> {
    const startedAt = new Date().toISOString();
    const defaults =
      "getDefaultOptions" in this.#agent &&
      typeof this.#agent.getDefaultOptions === "function"
        ? ((await this.#agent.getDefaultOptions({
            requestContext: callerOptions.requestContext,
          })) as RuntimeGenerateOptions)
        : {};
    if (structuredOutputUsesModel(defaults)) {
      throw new TypeError(
        "Kitaru cannot record agent-default structuredOutput.model. Move the secondary model configuration to the per-run generate options.",
      );
    }
    const hasSecondaryModel = structuredOutputUsesModel(callerOptions);
    if (
      hasSecondaryModel &&
      (!("getModel" in this.#agent) ||
        typeof this.#agent.getModel !== "function")
    ) {
      throw new TypeError(
        "Kitaru structuredOutput.model requires an agent with the public getModel() method",
      );
    }
    const structuredOutput = {
      ...(isRecord(defaults.structuredOutput) ? defaults.structuredOutput : {}),
      ...(isRecord(callerOptions.structuredOutput)
        ? Object.fromEntries(
            Object.entries(callerOptions.structuredOutput).filter(
              ([, value]) => value !== undefined,
            ),
          )
        : {}),
    };
    if (
      hasSecondaryModel &&
      (structuredOutput.useAgent === true ||
        (structuredOutput.errorStrategy !== undefined &&
          structuredOutput.errorStrategy !== "strict"))
    ) {
      throw new TypeError(
        "Kitaru secondary structured output requires useAgent: false and errorStrategy: strict. Conversation-aware structuring and suppressed validation failures are unsupported.",
      );
    }
    const requestedModelId =
      readableModelId(callerOptions.model) ?? this.#options.requestedModelId;
    const replay = await resolveReplayContext({
      allowedReplayModels: this.#options.allowedReplayModels,
      callerInput: callerMessages,
      client: this.#client,
      requestedModelId,
    });
    const needsContext =
      hasMemoryOptions(callerOptions) || hasMemoryOptions(defaults);
    const contextMessages = restoreConversationContext(replay.effectiveInput);
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
      (replay.override?.prompt != null ||
        replay.override?.system_prompt != null)
    ) {
      throw new Error(
        "Unsupported Mastra replay: prompt and system_prompt overrides cannot replace a recorded conversation context. Record a new invocation with the desired messages.",
      );
    }
    let effectiveMessages = contextMessages ?? replay.effectiveRuntimeInput;
    const effectiveOptions: RuntimeGenerateOptions = { ...callerOptions };
    if (contextMessages) {
      // The snapshot includes both instructions and memory-generated system messages.
      effectiveOptions.instructions = [];
      effectiveOptions.system = [];
      effectiveOptions.context = [];
    }
    let replayAbortController: AbortController | undefined;

    if (replay.replacementModelId !== undefined) {
      if (!this.#options.resolveModel) {
        throw new Error(
          `Cannot resolve replacement model '${replay.replacementModelId}' without resolveModel`,
        );
      }
      const resolved = await this.#options.resolveModel(
        replay.replacementModelId,
      );
      if (resolved === undefined || resolved === null) {
        throw new Error(
          `Replacement model '${replay.replacementModelId}' did not resolve`,
        );
      }
      effectiveOptions.model = resolved;
    }
    if (
      replay.override?.system_prompt !== undefined &&
      replay.override.system_prompt !== null
    ) {
      delete effectiveOptions.system;
      effectiveOptions.instructions = replay.override.system_prompt;
      effectiveMessages = stripSystemMessages(effectiveMessages);
    }
    const overrideModelSettings = parseModelSettings(
      replay.override?.model_params,
    );
    if (overrideModelSettings) {
      effectiveOptions.modelSettings = {
        ...callerOptions.modelSettings,
        ...overrideModelSettings,
      };
    }
    if (replay.spec) {
      replayAbortController = new AbortController();
      effectiveOptions.abortSignal = callerOptions.abortSignal
        ? AbortSignal.any([
            callerOptions.abortSignal,
            replayAbortController.signal,
          ])
        : replayAbortController.signal;
      stripLiveMemoryOptions(effectiveOptions);
      await assertReplayToolCoverage({
        agent: this.#agent,
        runtimeOptions: effectiveOptions,
        spec: replay.spec,
      });
      // Serial tool calls let a policy failure stop the run before another
      // tool in the same step executes for real.
      effectiveOptions.toolCallConcurrency = 1;
    }
    const effectiveModelSettings = serializedSettings(
      effectiveOptions.modelSettings,
    );

    let recorder: RunRecorder | undefined;
    let recordedInput =
      needsContext && !replay.spec
        ? createContextInput(replay.effectiveInput)
        : replay.effectiveInput;
    let recorderPromise: Promise<RunRecorder> | undefined;
    const initializeRecorder = (): Promise<RunRecorder> => {
      recorderPromise ??= (async () => {
        recorder = await RunRecorder.create({
          startedAt,
          adapterVersion: ADAPTER_VERSION,
          agentId: this.#options.agentId,
          agentVersionId: this.#options.agentVersionId,
          client: this.#client,
          effectiveInput: recordedInput,
          effectiveModelSettings,
          framework: "mastra",
          name: this.#sessionName,
          replayId: replay.replayId,
          requestedModelId,
          sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
          spec: replay.spec,
        });
        await recorder.initialize();
        return recorder;
      })();
      return recorderPromise;
    };
    const captureContext = needsContext && !replay.spec;
    let secondaryFailure: Error | undefined;
    if (hasSecondaryModel) {
      const getModel = (
        this.#agent as unknown as Pick<Agent, "getModel">
      ).getModel.bind(this.#agent);
      effectiveOptions.structuredOutput = {
        ...structuredOutput,
        model: await prepareStructuredOutputModel({
          modelConfig: structuredOutput.model as MastraModelConfig,
          resolveModel: (modelConfig) =>
            getModel({
              modelConfig,
              requestContext: callerOptions.requestContext,
            }),
          getState: async () => (await initializeRecorder()).state,
          costCalculator: this.#options.costCalculator,
          onAttemptFinish: (error) => {
            secondaryFailure = error;
          },
        }),
      };
    }
    if (captureContext) {
      const configuredProcessors =
        callerOptions.inputProcessors ??
        defaults.inputProcessors ??
        ("listConfiguredInputProcessors" in this.#agent &&
        typeof this.#agent.listConfiguredInputProcessors === "function"
          ? await this.#agent.listConfiguredInputProcessors(
              callerOptions.requestContext,
            )
          : undefined);
      if (Array.isArray(configuredProcessors)) {
        effectiveOptions.inputProcessors = [
          ...configuredProcessors,
          createContextProcessor(async (messages) => {
            recordedInput = createContextInput(
              replay.effectiveInput,
              configuredProcessors.length === 0 &&
                callerOptions.prepareStep === undefined &&
                defaults.prepareStep === undefined
                ? messages
                : undefined,
              "This recording used memory behavior or input transformations outside history-only replay support. Disable working, semantic, and observational memory, input processors, and prepareStep before recording a new invocation.",
            );
            await initializeRecorder();
          }),
        ];
      }
    }

    try {
      if (!captureContext) await initializeRecorder();

      let modelError: unknown;
      const callerOnError = effectiveOptions.onError;
      effectiveOptions.onError = async (event) => {
        modelError = event.error;
        await callerOnError?.(event);
      };
      const callerOnStepFinish = effectiveOptions.onStepFinish;
      effectiveOptions.onStepFinish = async (step) => {
        const recordedStep =
          step.finishReason === "error" && modelError !== undefined
            ? { ...step, error: modelError }
            : step;
        const state = (await initializeRecorder()).state;
        await recordStep(
          state,
          recordedStep as RecordedStep,
          this.#options.costCalculator,
        );
        if (step.finishReason === "error") {
          modelError = undefined;
        }
        await this.#options.configuredOnStepFinish?.(step);
        await callerOnStepFinish?.(step);
        // Mastra turns a tool-hook rejection into a tool-error result and keeps
        // looping, so the adapter stops the run itself once a policy failed.
        if (state.failure !== undefined) {
          throw state.failure;
        }
      };
      const getToolHooks = async () =>
        createToolHooks({
          abortReplay: (reason) => replayAbortController?.abort(reason),
          callerHooks: callerOptions.hooks,
          configuredAfterToolCall: this.#options.configuredAfterToolCall,
          configuredBeforeToolCall: this.#options.configuredBeforeToolCall,
          state: (await initializeRecorder()).state,
        });
      effectiveOptions.hooks = {
        beforeToolCall: async (event) =>
          (await getToolHooks()).beforeToolCall?.(event),
        afterToolCall: async (event) =>
          (await getToolHooks()).afterToolCall?.(event),
      };

      const generate = this.#agent.generate as unknown as RuntimeGenerate;
      const result = await generate.call(
        this.#agent,
        effectiveMessages,
        effectiveOptions,
      );
      const activeRecorder = await initializeRecorder();
      if (activeRecorder.state.failure !== undefined) {
        throw activeRecorder.state.failure;
      }
      const tripwire = tripwireReason(result);
      if (tripwire !== undefined) {
        await activeRecorder.fail(new Error(tripwire));
        return result;
      }
      if (secondaryFailure !== undefined) {
        await activeRecorder.fail(secondaryFailure);
        return result;
      }
      await activeRecorder.complete(
        boundedRecorderJson(
          runResultSummary(result, {
            structuredOutputField:
              (effectiveOptions.structuredOutput ??
                defaults.structuredOutput) === undefined
                ? undefined
                : "object",
          }),
          "generation result",
        ),
      );
      return result;
    } catch (error) {
      const primary = recorder?.state.failure ?? error;
      try {
        await (recorder ?? (await initializeRecorder())).fail(primary);
      } catch {
        // Recording cleanup must not replace the original runtime failure.
      }
      throw primary;
    }
  }
}
