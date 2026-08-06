import { KitaruClient } from "@zenml-io/kitaru";
import {
  RunRecorder,
  resolveReplayContext,
  serializedSettings,
} from "@zenml-io/kitaru/adapter";
import { type RecordedStep, recordStep } from "./step-recorder.js";
import { createReplayToolHooks } from "./tool-policies.js";
import type {
  GenerateCapable,
  GenerateMethod,
  KitaruAgentOptions,
  RuntimeGenerateOptions,
} from "./types.js";

const ADAPTER_VERSION = "0.1.0-experimental.0";

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
    const requestedModelId =
      readableModelId(callerOptions.model) ?? this.#options.requestedModelId;
    const replay = await resolveReplayContext({
      callerInput: callerMessages,
      client: this.#client,
      requestedModelId,
    });
    const effectiveMessages = replay.effectiveRuntimeInput;
    const effectiveOptions: RuntimeGenerateOptions = { ...callerOptions };

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
    }
    if (
      replay.override?.model_params !== undefined &&
      replay.override.model_params !== null
    ) {
      effectiveOptions.modelSettings = replay.override.model_params;
    }
    const effectiveModelSettings = serializedSettings(
      effectiveOptions.modelSettings,
    );

    const recorder = await RunRecorder.create({
      adapterVersion: ADAPTER_VERSION,
      agentId: this.#options.agentId,
      agentVersionId: this.#options.agentVersionId,
      client: this.#client,
      effectiveInput: replay.effectiveInput,
      effectiveModelSettings,
      framework: "mastra",
      name: this.#sessionName,
      replayId: replay.replayId,
      requestedModelId,
      sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
      spec: replay.spec,
    });
    const state = recorder.state;

    try {
      await recorder.initialize();

      const callerOnStepFinish = effectiveOptions.onStepFinish;
      effectiveOptions.onStepFinish = async (step) => {
        await recordStep(state, step as RecordedStep);
        await this.#options.configuredOnStepFinish?.(step);
        await callerOnStepFinish?.(step);
      };
      if (replay.spec) {
        effectiveOptions.hooks = createReplayToolHooks({
          callerHooks: callerOptions.hooks,
          configuredAfterToolCall: this.#options.configuredAfterToolCall,
          configuredBeforeToolCall: this.#options.configuredBeforeToolCall,
          state,
        });
      }

      const generate = this.#agent.generate as unknown as RuntimeGenerate;
      const result = await generate.call(
        this.#agent,
        effectiveMessages,
        effectiveOptions,
      );
      await recorder.complete(result);
      return result;
    } catch (error) {
      await recorder.fail(error);
      throw error;
    }
  }
}
