import type { KitaruEnvironmentVariables } from "@zenml-io/kitaru";
import type {
  AdapterClient,
  CostCalculator,
  CostInput,
} from "@zenml-io/kitaru/adapter";
import type {
  Agent,
  GenerateTextOnStepEndCallback,
  generateText,
  LanguageModel,
  Output,
  ToolLoopAgentSettings,
  ToolSet,
} from "ai";

export type KitaruGenerateText = typeof generateText;

export type KitaruToolLoopAgent<
  CALL_OPTIONS = never,
  TOOLS extends ToolSet = Record<never, never>,
  RUNTIME_CONTEXT extends Record<string, unknown> = Record<string, unknown>,
  OUTPUT extends Output.Output = never,
> = Agent<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT, OUTPUT>;

export type KitaruToolLoopAgentSettings<
  CALL_OPTIONS = never,
  TOOLS extends ToolSet = Record<never, never>,
  RUNTIME_CONTEXT extends Record<string, unknown> = Record<string, unknown>,
  OUTPUT extends Output.Output = never,
> = ToolLoopAgentSettings<CALL_OPTIONS, TOOLS, RUNTIME_CONTEXT, OUTPUT>;

export type KitaruCostInput = CostInput;
export type KitaruCostCalculator = CostCalculator;

export interface KitaruVercelAIOptions {
  agentId: string;
  agentVersionId?: string;
  allowedReplayModels?: readonly string[];
  apiKey?: string;
  apiUrl?: string;
  client?: AdapterClient;
  configuredOnStepEnd?: GenerateTextOnStepEndCallback<ToolSet>;
  costCalculator?: KitaruCostCalculator;
  environment?: KitaruEnvironmentVariables;
  fetch?: typeof globalThis.fetch;
  requestedModelId?: string;
  resolveModel?: (
    modelId: string,
  ) =>
    | LanguageModel
    | null
    | undefined
    | Promise<LanguageModel | null | undefined>;
  sessionName?: string;
  ticketTimeoutMs?: number;
  timeoutMs?: number;
}
