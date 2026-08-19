import type { KitaruEnvironmentVariables } from "../environment.js";
import { toRecorderJson } from "../json.js";
import type { JsonValue, ReplayOverride, ReplaySpec } from "../types.js";
import { isRecord, isUuid } from "../validation.js";
import type { AdapterClient } from "./run-state.js";

function parseUuidEnvironment(
  name: string,
  value: string | undefined,
): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isUuid(value)) {
    throw new TypeError(`${name} must be a UUID`);
  }
  return value;
}

export function parseReplayId(value: string | undefined): string | undefined {
  return parseUuidEnvironment("KITARU_REPLAY_ID", value);
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

/**
 * Drop the system messages a caller's message array already carries.
 *
 * A replaced system prompt reaches the model as instructions. A system message
 * left in the array would survive alongside it, so the run would answer with
 * the old prompt still in force and the replay would report no difference.
 */
export function stripSystemMessages(value: unknown): unknown {
  if (!Array.isArray(value)) {
    return value;
  }
  return value.filter(
    (message) => !isRecord(message) || message.role !== "system",
  );
}

export interface ReplayContext {
  effectiveInput: JsonValue;
  effectiveRuntimeInput: unknown;
  override?: ReplayOverride;
  replayId?: string;
  replacementModelId?: string;
  spec?: ReplaySpec;
}

async function resolveWorkerInput(options: {
  callerInput: unknown;
  client: AdapterClient;
  environment: KitaruEnvironmentVariables;
}): Promise<unknown> {
  const taskInputs = options.environment.KITARU_TASK_INPUTS;
  if (taskInputs !== undefined) {
    return parseJsonEnvironment("KITARU_TASK_INPUTS", taskInputs);
  }
  const taskId = parseUuidEnvironment(
    "KITARU_TASK_ID",
    options.environment.KITARU_TASK_ID,
  );
  if (taskId === undefined) {
    return options.callerInput;
  }
  const spec = await options.client.getTaskSpec(taskId);
  if (spec.kind !== "agent" || spec.details.kind !== "agent") {
    throw new TypeError(`Task ${taskId} is not an agent task`);
  }
  return spec.details.inputs;
}

interface EffectiveInputs {
  // What the session records: the inputs the server would have sent.
  recorded: unknown;
  // What the agent is handed: the same inputs without the system prompt, which
  // reaches the model through the adapter instead.
  runtime: unknown;
}

// The {prompt, system_prompt} wrapper is unwrapped wherever it appears,
// including when a previous replay recorded it, so replaying a replay hands
// the agent a prompt rather than the wrapper. A wrapper with no prompt key
// only loses its system prompt when this replay is the one that put it there:
// otherwise the key is part of the recorded input and stays.
function withoutSystemPrompt(
  recorded: unknown,
  systemPromptReplaced: boolean,
): unknown {
  if (!isRecord(recorded) || !Object.hasOwn(recorded, "system_prompt")) {
    return recorded;
  }
  if (Object.hasOwn(recorded, "prompt")) {
    return recorded.prompt;
  }
  if (!systemPromptReplaced) {
    return recorded;
  }
  const { system_prompt: _replaced, ...rest } = recorded;
  return rest;
}

// Mirrors effective_inputs in the server's replay config so a locally applied
// override produces the same inputs the server would have sent, and so
// re-applying the server's own override is a no-op.
function resolveEffectiveInputs(
  workerInput: unknown,
  override: ReplayOverride | undefined,
): EffectiveInputs {
  const prompt = override?.prompt ?? undefined;
  const systemPrompt = override?.system_prompt ?? undefined;
  if (prompt === undefined && systemPrompt === undefined) {
    return {
      recorded: workerInput,
      runtime: withoutSystemPrompt(workerInput, false),
    };
  }
  if (isRecord(workerInput)) {
    const recorded = {
      ...workerInput,
      ...(prompt === undefined ? {} : { prompt }),
      ...(systemPrompt === undefined ? {} : { system_prompt: systemPrompt }),
    };
    return {
      recorded,
      runtime: withoutSystemPrompt(recorded, systemPrompt !== undefined),
    };
  }
  if (systemPrompt === undefined) {
    return { recorded: prompt, runtime: prompt };
  }
  const runtime = prompt ?? workerInput;
  return {
    recorded: { prompt: runtime, system_prompt: systemPrompt },
    runtime,
  };
}

function assertAllowedReplayModel(
  replacementModelId: string,
  allowedReplayModels: readonly string[] | undefined,
): void {
  if (!allowedReplayModels?.includes(replacementModelId)) {
    throw new TypeError(
      `Replacement model '${replacementModelId}' is not in allowedReplayModels`,
    );
  }
}

export async function resolveReplayContext(options: {
  allowedReplayModels?: readonly string[];
  callerInput: unknown;
  client: AdapterClient;
  environment?: KitaruEnvironmentVariables;
  requestedModelId: string;
}): Promise<ReplayContext> {
  const environment = options.environment ?? process.env;
  const replayId = parseReplayId(environment.KITARU_REPLAY_ID);
  const spec = replayId ? await options.client.getReplay(replayId) : undefined;
  const workerInput = await resolveWorkerInput({
    callerInput: options.callerInput,
    client: options.client,
    environment,
  });
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
  const effective = resolveEffectiveInputs(workerInput, override);
  const replacementModelId = modelReplacement(
    override,
    options.requestedModelId,
  );
  if (replacementModelId !== undefined) {
    // A replay override that swaps the model decides what every session in a
    // batch spends, so the allowlist is checked before anything is recorded.
    assertAllowedReplayModel(replacementModelId, options.allowedReplayModels);
  }

  return {
    effectiveInput: toRecorderJson(effective.recorded),
    effectiveRuntimeInput: effective.runtime,
    override,
    replayId,
    replacementModelId,
    spec,
  };
}
