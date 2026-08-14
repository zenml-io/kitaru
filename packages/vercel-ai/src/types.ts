import type { KitaruEnvironmentVariables } from "@zenml-io/kitaru";
import type {
  AdapterClient,
  CostCalculator,
  CostInput,
} from "@zenml-io/kitaru/adapter";
import type {
  GenerateTextOnStepEndCallback,
  generateText,
  LanguageModel,
  ToolSet,
} from "ai";

export type KitaruGenerateText = typeof generateText;

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
