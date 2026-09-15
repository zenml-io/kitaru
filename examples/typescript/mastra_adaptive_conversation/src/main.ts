import { openai } from "@ai-sdk/openai";
import { Agent } from "@mastra/core/agent";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  type AdapterClient,
  RunRecorder,
  resolveReplayContext,
} from "@zenml-io/kitaru/adapter";
import { createKitaruClient } from "@zenml-io/kitaru/node";
import {
  DEFAULT_PROMPT,
  DEFAULT_SYSTEM,
  LIMITS,
  MODEL,
  runConversation,
  type TargetConfig,
} from "./conversation.js";

export function resolveTarget(
  override:
    | {
        system_prompt?: string | null;
        model_params?: Record<string, unknown> | null;
      }
    | undefined,
  prompt = DEFAULT_SYSTEM,
  model = MODEL,
): TargetConfig {
  const params = override?.model_params ?? {};
  if (Object.keys(params).some((key) => key !== "maxOutputTokens"))
    throw new Error("Only maxOutputTokens target settings are supported");
  const tokens = params.maxOutputTokens ?? LIMITS.maxOutputTokens;
  if (
    typeof tokens !== "number" ||
    !Number.isInteger(tokens) ||
    tokens < 1 ||
    tokens > LIMITS.maxOutputTokens
  )
    throw new Error("maxOutputTokens must be an integer from 1 to 2000");
  const system = override?.system_prompt ?? prompt;
  if (system.length > LIMITS.textChars)
    throw new Error("Target system prompt too long");
  return { model, system, maxOutputTokens: tokens };
}

export async function resolveConfiguration(
  client: AdapterClient,
  environment: Parameters<
    typeof resolveReplayContext
  >[0]["environment"] = process.env,
) {
  const context = await resolveReplayContext({
    callerInput: {
      scenario_version: "parcel-fixture-v1",
      prompt: DEFAULT_SYSTEM,
    },
    client,
    environment,
    requestedModelId: MODEL,
    allowedReplayModels: [MODEL, "openai/gpt-5-mini"],
  });
  const policy = context.spec?.tool_policy;
  if (
    policy &&
    (policy.default.type !== "passthrough" ||
      Object.keys(policy.tools ?? {}).length > 0)
  )
    throw new Error(
      "Raw tool-free calls support only the default passthrough policy",
    );
  const input = context.effectiveInput;
  if (
    typeof input !== "object" ||
    input === null ||
    Array.isArray(input) ||
    input.scenario_version !== "parcel-fixture-v1"
  )
    throw new Error("Expected versioned parcel-fixture-v1 input object");
  const prompt = input.prompt;
  if (
    typeof prompt !== "string" ||
    !prompt.trim() ||
    prompt.length > LIMITS.textChars
  )
    throw new Error(
      "Worker input must be a nonempty prompt string up to 6000 characters",
    );
  const target = resolveTarget(
    context.override,
    prompt,
    context.replacementModelId ?? MODEL,
  );
  return { context, target };
}

async function main(): Promise<void> {
  const agentId = process.env.KITARU_AGENT_ID;
  if (!agentId || !process.env.OPENAI_API_KEY)
    throw new Error("KITARU_AGENT_ID and OPENAI_API_KEY are required");
  const client = await createKitaruClient();
  const { context, target } = await resolveConfiguration(client);
  const recorder = await RunRecorder.create({
    adapterVersion: "example-v1",
    agentId,
    agentVersionId: process.env.KITARU_AGENT_VERSION_ID,
    client,
    effectiveInput: context.effectiveInput,
    framework: "mastra",
    name: "Adaptive fixture conversation",
    replayId: context.replayId,
    requestedModelId: MODEL,
    sessionIdFile: process.env.KITARU_SESSION_ID_FILE,
    spec: context.spec,
  });
  try {
    await recorder.initialize();
    const result = await runConversation({
      prompt: DEFAULT_PROMPT,
      target,
      checkpoint: async (transcript) => {
        await client.updateSession(recorder.state.sessionId, {
          outputs: JSON.parse(JSON.stringify(transcript)) as JsonValue,
        });
      },
      createTarget: (config) => {
        const agent = new Agent({
          id: "adaptive-parcel-fixture",
          name: "Adaptive parcel fixture",
          instructions: config.system,
          model: openai(config.model.slice("openai/".length)),
          tools: {},
        });
        return async (history, signal) => {
          const response = await agent.generate(history, {
            abortSignal: signal,
            maxSteps: LIMITS.stepsPerCall,
            modelSettings: {
              maxOutputTokens: config.maxOutputTokens,
              maxRetries: 0,
            },
          });
          return {
            text: response.text,
            totalTokens: response.totalUsage.totalTokens ?? Number.NaN,
          };
        };
      },
    });
    await recorder.complete(result);
    console.log(
      JSON.stringify({
        session_id: recorder.state.sessionId,
        stop_reason: result.stop_reason,
      }),
    );
  } catch {
    // Provider errors can include request bodies or credentials; store a fixed error.
    await recorder.fail(
      new Error("Adaptive conversation failed; inspect the partial transcript"),
    );
    throw new Error(
      "Adaptive conversation failed; inspect the partial transcript",
    );
  }
}

if (process.argv[1]?.endsWith("/main.js")) await main();
