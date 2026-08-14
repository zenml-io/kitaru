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
  type AgentCallParameters,
  type AgentStreamParameters,
  type GenerateTextResult,
  type LanguageModel,
  type LanguageModelCallEndEvent,
  type LanguageModelCallStartEvent,
  type Output,
  type StepResult,
  type StreamTextResult,
  ToolLoopAgent,
  type ToolLoopAgentSettings,
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
import type { KitaruToolLoopAgent, KitaruVercelAIOptions } from "./types.js";

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

type RuntimeSettings = Record<string, unknown> & {
  messages?: unknown;
  model: LanguageModel;
  prompt?: unknown;
  tools?: ToolSet;
};

interface InvocationState {
  currentModelSettings?: Record<string, JsonValue>;
  includeOutput?: boolean;
  pendingModelCall?: Pick<
    LanguageModelCallStartEvent,
    "callId" | "modelId" | "provider"
  > & {
    modelSettings?: Record<string, JsonValue>;
    startedAt: string;
  };
  recorder?: Awaited<ReturnType<typeof RunRecorder.create>>;
  tickets?: ExecutionTickets;
}

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

function effectiveInput(settings: RuntimeSettings): unknown {
  return settings.prompt !== undefined ? settings.prompt : settings.messages;
}

function applyOverride(
  settings: RuntimeSettings,
  override: SafeReplayOverride | undefined,
  taskInput: string | JsonValue[] | undefined,
): void {
  const effectivePrompt =
    override?.prompt ?? (typeof taskInput === "string" ? taskInput : undefined);
  if (effectivePrompt !== undefined) {
    settings.prompt = effectivePrompt;
    delete settings.messages;
  } else if (taskInput !== undefined) {
    settings.messages = taskInput;
    delete settings.prompt;
  }
  if (override?.systemPrompt !== undefined) {
    settings.instructions = override.systemPrompt;
    delete settings.system;
    if (settings.messages !== undefined) {
      settings.messages = stripSystemMessages(settings.messages);
    }
    if (Array.isArray(settings.prompt)) {
      settings.prompt = stripSystemMessages(settings.prompt);
    }
  }
  if (override?.modelSettings) {
    Object.assign(settings, override.modelSettings);
  }
}

function hasApprovalRequest(result: {
  content: readonly { type: string }[];
}): boolean {
  return result.content.some((part) => part.type === "tool-approval-request");
}

function resultSummary(result: unknown, includeOutput: boolean): JsonValue {
  return recordedPayloadJson(
    runResultSummary(result, {
      structuredOutputField: includeOutput ? "output" : undefined,
    }),
    "generation result",
  );
}

/** Create an AI SDK ToolLoopAgent whose non-streaming calls are recorded by Kitaru. */
export function createKitaruToolLoopAgent<
  CALL_OPTIONS = never,
  TOOLS extends ToolSet = Record<never, never>,
  RUNTIME_CONTEXT extends Record<string, unknown> = Record<string, unknown>,
  OUTPUT extends Output.Output = never,
>(
  settings: ToolLoopAgentSettings<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT, OUTPUT>,
  adapterOptions: KitaruVercelAIOptions,
): KitaruToolLoopAgent<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT, OUTPUT> {
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

  function invocationAgent(state: InvocationState) {
    const callerPrepareCall = settings.prepareCall;
    const callerOnStepEnd = settings.onStepEnd ?? settings.onStepFinish;
    const invocationSettings = {
      ...settings,
      onStepEnd: async (step: StepResult<TOOLS>) => {
        const runState = state.recorder?.state;
        if (runState === undefined) {
          throw new Error("Kitaru recorder was not initialized");
        }
        try {
          await recordVercelStep(
            runState,
            step as StepResult<ToolSet>,
            adapterOptions.costCalculator,
            state.currentModelSettings,
          );
          state.currentModelSettings = undefined;
        } catch (error) {
          runState.storeFailure(error);
          throw error;
        }
        await adapterOptions.configuredOnStepEnd?.(step as StepResult<ToolSet>);
        await (
          callerOnStepEnd as ((value: StepResult<TOOLS>) => unknown) | undefined
        )?.(step);
      },
      onStepFinish: undefined,
      prepareCall: async (baseCall: unknown) => {
        const prepared = callerPrepareCall
          ? await callerPrepareCall.call(settings, baseCall as never)
          : baseCall;
        const runtime = { ...(prepared as RuntimeSettings) };
        const requestedModelId =
          adapterOptions.requestedModelId ?? readableModelId(runtime.model);
        if (!requestedModelId) {
          throw new TypeError(
            "requestedModelId is required when the model has no readable modelId",
          );
        }
        const callerInput = effectiveInput(runtime);
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
        const taskInput =
          replay.effectiveRuntimeInput === callerInput
            ? undefined
            : parseWorkerTaskInput(replay.effectiveRuntimeInput);
        applyOverride(runtime, override, taskInput);
        if (replay.replacementModelId !== undefined) {
          if (!adapterOptions.resolveModel) {
            throw new TypeError(
              `Cannot resolve replacement model '${replay.replacementModelId}' without resolveModel`,
            );
          }
          const replacement = await adapterOptions.resolveModel(
            replay.replacementModelId,
          );
          if (replacement === undefined || replacement === null) {
            throw new TypeError(
              `Replacement model '${replay.replacementModelId}' did not resolve`,
            );
          }
          runtime.model = replacement;
        }
        if (replay.spec) {
          if (
            hasApprovalResponse(runtime.messages) ||
            hasApprovalResponse(runtime.prompt)
          ) {
            throw new TypeError(
              "Replay does not support pre-supplied tool approval messages",
            );
          }
          assertSupportedReplayTools({
            generationOptions: runtime,
            spec: replay.spec,
            tools: runtime.tools,
          });
        }
        state.includeOutput = runtime.output !== undefined;

        const recorder = await RunRecorder.create({
          adapterVersion: ADAPTER_VERSION,
          agentId: adapterOptions.agentId,
          agentVersionId: adapterOptions.agentVersionId,
          client,
          effectiveInput: projectRecordedInput(replay.effectiveInput),
          effectiveModelSettings: callerModelSettings(runtime),
          framework: "vercel-ai-sdk",
          name: adapterOptions.sessionName ?? environment.KITARU_SESSION_NAME,
          replayId: replay.replayId,
          requestedModelId,
          sessionIdFile: environment.KITARU_SESSION_ID_FILE,
          spec: replay.spec,
        });
        state.recorder = recorder;
        state.tickets = new ExecutionTickets(adapterOptions.ticketTimeoutMs);
        await recorder.initialize();

        const callerOnLanguageModelCallStart =
          runtime.onLanguageModelCallStart ??
          runtime.experimental_onLanguageModelCallStart;
        const callerOnLanguageModelCallEnd =
          runtime.onLanguageModelCallEnd ??
          runtime.experimental_onLanguageModelCallEnd;
        delete runtime.experimental_onLanguageModelCallStart;
        delete runtime.experimental_onLanguageModelCallEnd;

        const wrappedTools = wrapTools({
          state: recorder.state,
          tickets: state.tickets,
          tools: runtime.tools,
        });
        runtime.tools = wrappedTools;
        runtime.stopWhen = composeStopConditions(
          recorder.state,
          runtime.stopWhen as never,
        );
        runtime.onLanguageModelCallStart = async (
          event: LanguageModelCallStartEvent,
        ) => {
          state.pendingModelCall = {
            callId: event.callId,
            modelId: event.modelId,
            modelSettings: callerModelSettings(
              event as unknown as Record<string, unknown>,
            ),
            provider: event.provider,
            startedAt: new Date().toISOString(),
          };
          state.currentModelSettings = state.pendingModelCall.modelSettings;
          await (
            callerOnLanguageModelCallStart as
              | ((value: LanguageModelCallStartEvent) => unknown)
              | undefined
          )?.(event);
        };
        runtime.onLanguageModelCallEnd = async (
          event: LanguageModelCallEndEvent<ToolSet>,
        ) => {
          state.pendingModelCall = undefined;
          if (replay.spec) {
            registerReplayTickets({
              event,
              state: recorder.state,
              tickets: state.tickets as ExecutionTickets,
              tools: wrappedTools,
            });
          }
          await (
            callerOnLanguageModelCallEnd as
              | ((value: LanguageModelCallEndEvent<ToolSet>) => unknown)
              | undefined
          )?.(event);
        };
        return runtime as never;
      },
    } satisfies ToolLoopAgentSettings<
      CALL_OPTIONS,
      TOOLS,
      RUNTIME_CONTEXT,
      OUTPUT
    >;
    return new ToolLoopAgent(invocationSettings);
  }

  return {
    version: "agent-v1",
    id: settings.id,
    tools: settings.tools as TOOLS,
    async generate(
      options: AgentCallParameters<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT>,
    ): Promise<GenerateTextResult<TOOLS, RUNTIME_CONTEXT, OUTPUT>> {
      const state: InvocationState = {};
      try {
        const result = await invocationAgent(state).generate(options);
        if (state.recorder === undefined || state.tickets === undefined) {
          throw new Error("Kitaru recorder was not initialized");
        }
        if (state.recorder.state.failure !== undefined) {
          throw state.recorder.state.failure;
        }
        if (state.recorder.state.spec) {
          state.tickets.assertConsumed();
        }
        if (hasApprovalRequest(result)) {
          await state.recorder.fail(
            new Error("manual_approval_continuation_unsupported"),
          );
          return result;
        }
        await state.recorder.complete(
          resultSummary(result, state.includeOutput === true),
        );
        return result;
      } catch (error) {
        const primary = state.recorder?.state.failure ?? error;
        if (state.pendingModelCall !== undefined && state.recorder) {
          try {
            await recordFailedVercelModelCall(
              state.recorder.state,
              state.pendingModelCall,
              primary,
            );
          } catch {
            // Preserve the runtime failure if best-effort trace recording fails.
          }
        }
        await state.recorder?.fail(primary);
        throw primary;
      } finally {
        state.tickets?.cancel(new Error("Kitaru generation finished"));
      }
    },
    stream(
      options: AgentStreamParameters<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT>,
    ): PromiseLike<StreamTextResult<TOOLS, RUNTIME_CONTEXT, OUTPUT>> {
      return new ToolLoopAgent(settings).stream(options);
    },
  };
}
