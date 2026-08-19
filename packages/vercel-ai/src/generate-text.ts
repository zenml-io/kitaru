import { createRequire } from "node:module";
import { type JsonValue, KitaruClient } from "@zenml-io/kitaru";
import {
  projectRecordedInput,
  RunRecorder,
  recordedPayloadJson,
  resolveReplayContext,
  runResultSummary,
  stripSystemMessages,
} from "@zenml-io/kitaru/adapter";
import {
  generateText,
  type LanguageModel,
  type LanguageModelCallEndEvent,
  type LanguageModelCallStartEvent,
  type StepResult,
  type ToolSet,
} from "ai";

import { composeStopConditions } from "./failure.js";
import {
  callerModelSettings,
  parseVercelReplayOverride,
  parseWorkerTaskInput,
  type SafeReplayOverride,
} from "./options.js";
import {
  recordFailedVercelModelCall,
  recordVercelStep,
} from "./step-recorder.js";
import { ExecutionTickets } from "./tickets.js";
import {
  assertSupportedReplayTools,
  hasApprovalResponse,
  registerReplayTickets,
  wrapTools,
} from "./tool-wrapper.js";
import type { KitaruGenerateText, KitaruVercelAIOptions } from "./types.js";

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

type RuntimeOptions = Record<string, unknown> & {
  experimental_onLanguageModelCallEnd?: RuntimeCallback<
    LanguageModelCallEndEvent<ToolSet>
  >;
  experimental_onLanguageModelCallStart?: RuntimeCallback<LanguageModelCallStartEvent>;
  messages?: unknown;
  model: LanguageModel;
  onLanguageModelCallEnd?: RuntimeCallback<LanguageModelCallEndEvent<ToolSet>>;
  onLanguageModelCallStart?: RuntimeCallback<LanguageModelCallStartEvent>;
  onStepEnd?: RuntimeCallback<StepResult<ToolSet>>;
  onStepFinish?: RuntimeCallback<StepResult<ToolSet>>;
  prompt?: unknown;
  tools?: ToolSet;
};

type RuntimeCallback<T> = (value: T) => Promise<unknown> | unknown;

type RuntimeResult = {
  finishReason?: unknown;
  steps?: unknown[];
  text?: unknown;
};

function readableModelId(model: unknown): string | undefined {
  if (typeof model === "string") {
    return model;
  }
  if (typeof model !== "object" || model === null) {
    return undefined;
  }
  const candidate = model as Record<string, unknown>;
  return typeof candidate.modelId === "string" ? candidate.modelId : undefined;
}

function effectiveRuntimeInput(options: RuntimeOptions): unknown {
  return options.prompt !== undefined ? options.prompt : options.messages;
}

function safeResultSummary(
  result: RuntimeResult,
  includeOutput: boolean,
): JsonValue {
  return recordedPayloadJson(
    runResultSummary(result, {
      structuredOutputField: includeOutput ? "output" : undefined,
    }),
    "generation result",
  );
}

function applyOverride(
  options: RuntimeOptions,
  override: SafeReplayOverride | undefined,
  taskInput: string | JsonValue[] | undefined,
): void {
  const effectivePrompt =
    override?.prompt ?? (typeof taskInput === "string" ? taskInput : undefined);
  if (effectivePrompt !== undefined) {
    options.prompt = effectivePrompt;
    delete options.messages;
  } else if (taskInput !== undefined) {
    options.messages = taskInput;
    delete options.prompt;
  }
  if (override?.systemPrompt !== undefined) {
    options.instructions = override.systemPrompt;
    delete options.system;
    // A message array can carry its own system message, which would otherwise
    // survive alongside the replacement instructions and hide the override.
    if (options.messages !== undefined) {
      options.messages = stripSystemMessages(options.messages);
    }
    if (Array.isArray(options.prompt)) {
      options.prompt = stripSystemMessages(options.prompt);
    }
  }
  if (override?.modelSettings) {
    Object.assign(options, override.modelSettings);
  }
}

async function resolveReplacementModel(options: {
  adapter: KitaruVercelAIOptions;
  modelId: string | undefined;
}): Promise<LanguageModel | undefined> {
  if (options.modelId === undefined) {
    return undefined;
  }
  if (!options.adapter.resolveModel) {
    throw new TypeError(
      `Cannot resolve replacement model '${options.modelId}' without resolveModel`,
    );
  }
  const model = await options.adapter.resolveModel(options.modelId);
  if (model === undefined || model === null) {
    throw new TypeError(
      `Replacement model '${options.modelId}' did not resolve`,
    );
  }
  return model;
}

export function createKitaruGenerateText(
  adapterOptions: KitaruVercelAIOptions,
): KitaruGenerateText {
  const environment = adapterOptions.environment ?? process.env;
  const client =
    adapterOptions.client ??
    new KitaruClient({
      apiKey:
        adapterOptions.apiKey ??
        environment.KITARU_API_TOKEN ??
        environment.KITARU_API_KEY,
      apiUrl: adapterOptions.apiUrl ?? environment.KITARU_API_URL,
      fetch: adapterOptions.fetch,
      timeoutMs: adapterOptions.timeoutMs,
    });

  const boundGenerateText = async (
    callerOptions: RuntimeOptions,
  ): Promise<unknown> => {
    const requestedModelId =
      adapterOptions.requestedModelId ?? readableModelId(callerOptions.model);
    if (!requestedModelId) {
      throw new TypeError(
        "requestedModelId is required when the model has no readable modelId",
      );
    }

    const callerInput = effectiveRuntimeInput(callerOptions);
    const replay = await resolveReplayContext({
      allowedReplayModels: adapterOptions.allowedReplayModels,
      callerInput,
      client,
      environment,
      requestedModelId,
    });
    const override = parseVercelReplayOverride(
      replay.override,
      "replay override",
    );
    // The worker-input bounds apply to input Kitaru injects, not to the
    // caller's own prompt, which the adapter passes through untouched.
    const taskInput =
      replay.effectiveRuntimeInput === callerInput
        ? undefined
        : parseWorkerTaskInput(replay.effectiveRuntimeInput);
    const effectiveOptions: RuntimeOptions = { ...callerOptions };
    applyOverride(effectiveOptions, override, taskInput);

    const replacementModel = await resolveReplacementModel({
      adapter: adapterOptions,
      modelId: replay.replacementModelId,
    });
    if (replacementModel) {
      effectiveOptions.model = replacementModel;
    }

    if (replay.spec) {
      if (hasApprovalResponse(effectiveOptions.messages)) {
        throw new TypeError(
          "Replay does not support pre-supplied tool approval messages",
        );
      }
      assertSupportedReplayTools({
        generationOptions: effectiveOptions,
        spec: replay.spec,
        tools: effectiveOptions.tools,
      });
    }

    const recordedInput = projectRecordedInput(replay.effectiveInput);
    const effectiveModelSettings = callerModelSettings(effectiveOptions);
    const recorder = await RunRecorder.create({
      adapterVersion: ADAPTER_VERSION,
      agentId: adapterOptions.agentId,
      agentVersionId: adapterOptions.agentVersionId,
      client,
      effectiveInput: recordedInput,
      effectiveModelSettings,
      framework: "vercel-ai-sdk",
      name: adapterOptions.sessionName ?? environment.KITARU_SESSION_NAME,
      replayId: replay.replayId,
      requestedModelId,
      sessionIdFile: environment.KITARU_SESSION_ID_FILE,
      spec: replay.spec,
      taskId: replay.taskId,
    });
    const tickets = new ExecutionTickets(adapterOptions.ticketTimeoutMs);
    const state = recorder.state;
    let pendingModelCall:
      | (Pick<
          LanguageModelCallStartEvent,
          "callId" | "modelId" | "provider"
        > & { startedAt: string })
      | undefined;

    try {
      await recorder.initialize();
      const callerOnLanguageModelCallStart =
        effectiveOptions.onLanguageModelCallStart ??
        effectiveOptions.experimental_onLanguageModelCallStart;
      const callerOnLanguageModelCallEnd =
        effectiveOptions.onLanguageModelCallEnd ??
        effectiveOptions.experimental_onLanguageModelCallEnd;
      const callerOnStepEnd =
        effectiveOptions.onStepEnd ?? effectiveOptions.onStepFinish;

      delete effectiveOptions.experimental_onLanguageModelCallStart;
      delete effectiveOptions.experimental_onLanguageModelCallEnd;
      delete effectiveOptions.onStepFinish;

      const wrappedTools = wrapTools({
        state,
        tickets,
        tools: effectiveOptions.tools,
      });
      effectiveOptions.tools = wrappedTools;
      effectiveOptions.maxRetries = 0;
      effectiveOptions.stopWhen = composeStopConditions(
        state,
        effectiveOptions.stopWhen as never,
      );
      effectiveOptions.onLanguageModelCallStart = async (
        event: LanguageModelCallStartEvent,
      ) => {
        await callerOnLanguageModelCallStart?.(event);
        pendingModelCall = {
          callId: event.callId,
          modelId: event.modelId,
          provider: event.provider,
          startedAt: new Date().toISOString(),
        };
      };
      effectiveOptions.onLanguageModelCallEnd = async (
        event: LanguageModelCallEndEvent<ToolSet>,
      ) => {
        pendingModelCall = undefined;
        if (replay.spec) {
          registerReplayTickets({
            event,
            state,
            tickets,
            tools: wrappedTools,
          });
        }
        await callerOnLanguageModelCallEnd?.(event);
      };
      effectiveOptions.onStepEnd = async (step: StepResult<ToolSet>) => {
        try {
          await recordVercelStep(state, step, adapterOptions.costCalculator);
        } catch (error) {
          state.storeFailure(error);
          throw error;
        }
        await adapterOptions.configuredOnStepEnd?.(step);
        await callerOnStepEnd?.(step);
      };

      const result = (await (
        generateText as unknown as (
          options: RuntimeOptions,
        ) => Promise<RuntimeResult>
      )(effectiveOptions)) as RuntimeResult;
      if (state.failure !== undefined) {
        throw state.failure;
      }
      if (replay.spec) {
        tickets.assertConsumed();
      }
      await recorder.complete(
        safeResultSummary(result, effectiveOptions.output !== undefined),
      );
      return result;
    } catch (error) {
      const primary = state.failure ?? error;
      if (pendingModelCall !== undefined) {
        try {
          await recordFailedVercelModelCall(state, pendingModelCall, primary);
        } catch {
          // Preserve the runtime failure if best-effort trace recording fails.
        }
      }
      await recorder.fail(primary);
      throw primary;
    } finally {
      tickets.cancel(new Error("Kitaru generation finished"));
    }
  };

  return boundGenerateText as KitaruGenerateText;
}
