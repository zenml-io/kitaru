import type { Agent } from "@mastra/core/agent";
import type { MastraModelConfig } from "@mastra/core/llm";
import type { LLMStepResult } from "@mastra/core/stream";
import type { ToolHooks } from "@mastra/core/tools";
import type { KitaruEnvironmentOptions } from "@zenml-io/kitaru";
import type { CostCalculator, CostInput } from "@zenml-io/kitaru/adapter";

export type KitaruCostInput = CostInput;
export type KitaruCostCalculator = CostCalculator;

export type ConfiguredOnStepFinish = (
  step: LLMStepResult<unknown> & {
    model?: PublicModelIdentity;
    runId?: string;
  },
) => Promise<void> | void;
export type ConfiguredBeforeToolCall = NonNullable<ToolHooks["beforeToolCall"]>;
export type ConfiguredAfterToolCall = NonNullable<ToolHooks["afterToolCall"]>;

export interface KitaruAgentOptions extends KitaruEnvironmentOptions {
  agentId: string;
  agentVersionId?: string;
  allowedReplayModels?: readonly string[];
  costCalculator?: KitaruCostCalculator;
  requestedModelId: string;
  resolveModel?: (
    replacementModelId: string,
  ) => MastraModelConfig | Promise<MastraModelConfig>;
  sessionName?: string;
  configuredOnStepFinish?: ConfiguredOnStepFinish;
  configuredBeforeToolCall?: ConfiguredBeforeToolCall;
  configuredAfterToolCall?: ConfiguredAfterToolCall;
}

export interface GenerateCapable {
  generate: (...args: never[]) => unknown;
}

export type MastraAgent = Agent;
export type GenerateMethod<TAgent extends GenerateCapable> = TAgent["generate"];

export interface RuntimeGenerateOptions {
  hooks?: ToolHooks;
  instructions?: unknown;
  model?: unknown;
  modelSettings?: Record<string, unknown>;
  onStepFinish?: ConfiguredOnStepFinish;
  system?: unknown;
  [key: string]: unknown;
}

export interface PublicModelIdentity {
  modelId?: string;
  provider?: string;
  version?: string;
}
