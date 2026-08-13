import type { KitaruEnvironmentVariables } from "../environment.js";
import { toRecorderJson } from "../json.js";
import type { JsonValue, ReplayOverride, ReplaySpec } from "../types.js";
import type { AdapterClient } from "./run-state.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseReplayId(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!UUID_PATTERN.test(value)) {
    throw new TypeError("KITARU_REPLAY_ID must be a UUID");
  }
  return value;
}

export function parseJsonEnvironment(name: string, value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch (error) {
    throw new TypeError(`${name} must contain valid JSON`, { cause: error });
  }
}

export function parseReplayOverride(
  value: unknown,
  source: string,
): ReplayOverride {
  if (!isRecord(value)) {
    throw new TypeError(`${source} must contain a JSON object`);
  }
  if (
    value.model !== undefined &&
    value.model !== null &&
    typeof value.model !== "string" &&
    !isRecord(value.model)
  ) {
    throw new TypeError(`${source}.model must be a string or object`);
  }
  if (
    isRecord(value.model) &&
    Object.values(value.model).some(
      (replacement) => typeof replacement !== "string",
    )
  ) {
    throw new TypeError(`${source}.model values must be strings`);
  }
  if (
    value.model_params !== undefined &&
    value.model_params !== null &&
    !isRecord(value.model_params)
  ) {
    throw new TypeError(`${source}.model_params must be an object`);
  }
  if (
    value.system_prompt !== undefined &&
    value.system_prompt !== null &&
    typeof value.system_prompt !== "string"
  ) {
    throw new TypeError(`${source}.system_prompt must be a string`);
  }
  if (
    value.prompt !== undefined &&
    value.prompt !== null &&
    typeof value.prompt !== "string"
  ) {
    throw new TypeError(`${source}.prompt must be a string`);
  }
  return value as ReplayOverride;
}

export function modelReplacement(
  override: ReplayOverride | undefined,
  requestedModelId: string,
): string | undefined {
  if (typeof override?.model === "string") {
    return override.model;
  }
  if (isRecord(override?.model)) {
    const replacement = override.model[requestedModelId];
    return typeof replacement === "string" ? replacement : undefined;
  }
  return undefined;
}

export interface ReplayContext {
  effectiveInput: JsonValue;
  effectiveRuntimeInput: unknown;
  override?: ReplayOverride;
  replayId?: string;
  replacementModelId?: string;
  spec?: ReplaySpec;
}

export async function resolveReplayContext(options: {
  callerInput: unknown;
  client: AdapterClient;
  environment?: KitaruEnvironmentVariables;
  requestedModelId: string;
}): Promise<ReplayContext> {
  const environment = options.environment ?? process.env;
  const replayId = parseReplayId(environment.KITARU_REPLAY_ID);
  const spec = replayId ? await options.client.getReplay(replayId) : undefined;
  const taskInputs = environment.KITARU_TASK_INPUTS;
  const workerInput =
    taskInputs === undefined
      ? options.callerInput
      : parseJsonEnvironment("KITARU_TASK_INPUTS", taskInputs);
  const override = spec
    ? spec.override
      ? parseReplayOverride(spec.override, "replay override")
      : undefined
    : environment.KITARU_OVERRIDE
      ? parseReplayOverride(
          parseJsonEnvironment("KITARU_OVERRIDE", environment.KITARU_OVERRIDE),
          "KITARU_OVERRIDE",
        )
      : undefined;
  const effectiveRuntimeInput = override?.prompt ?? workerInput;
  const effectiveInput = toRecorderJson(effectiveRuntimeInput);

  return {
    effectiveInput,
    effectiveRuntimeInput,
    override,
    replayId,
    replacementModelId: modelReplacement(override, options.requestedModelId),
    spec,
  };
}
