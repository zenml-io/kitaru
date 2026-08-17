import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function replaySpec(override: Record<string, unknown> | null) {
  return {
    baseline_session_id: ORIGINAL_SESSION_ID,
    id: REPLAY_ID,
    override,
    status: "pending",
    tool_policy: { default: { type: "passthrough" }, tools: {} },
  };
}

describe("replay overrides", () => {
  it("uses replay precedence and public per-run override options", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify("worker prompt"));
    vi.stubEnv(
      "KITARU_OVERRIDE",
      JSON.stringify({ model: "ignored-model", system_prompt: "ignored" }),
    );
    const api = installTestApi({
      replaySpec: replaySpec({
        model: { "caller-model": "replacement-model" },
        model_params: { temperature: 0.8 },
        prompt: "replacement prompt",
        system_prompt: "replacement instructions",
      }),
    });
    const resolver = vi.fn(async (modelId: string) => modelId);
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      allowedReplayModels: ["replacement-model"],
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
      resolveModel: resolver,
    });

    await recorded.generate("caller input", {
      maxSteps: 4,
      model: "caller-model",
      modelSettings: { temperature: 0.1, topP: 0.2 },
      system: "caller system",
    });

    expect(resolver).toHaveBeenCalledWith("replacement-model");
    expect(agent.calls[0]).toMatchObject({
      messages: "replacement prompt",
      options: {
        instructions: "replacement instructions",
        maxSteps: 4,
        model: "replacement-model",
        modelSettings: { temperature: 0.8 },
      },
    });
    expect(agent.calls[0]?.options).not.toHaveProperty("system");
    expect(
      api.calls.find((call) => call.path === "/api/v1/sessions")?.body,
    ).toMatchObject({
      inputs: {
        prompt: "replacement prompt",
        system_prompt: "replacement instructions",
      },
      origin: "replay",
    });
    const llm = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "llm_call");
    expect(llm).toMatchObject({
      model_params: { temperature: 0.8 },
      requested_model: "caller-model",
    });
  });

  it("replaces a system message carried inside a message-array input", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const messages = [
      { content: "Always answer in French", role: "system" },
      { content: "baseline prompt", role: "user" },
    ];
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(messages));
    installTestApi({
      replaySpec: replaySpec({ system_prompt: "Always answer in English" }),
    });
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
    });

    await recorded.generate("caller input");

    // A system message left in the array would shadow the replacement
    // instructions, and the replay would report no meaningful difference.
    expect(agent.calls[0]?.messages).toEqual([
      { content: "baseline prompt", role: "user" },
    ]);
    expect(agent.calls[0]?.options.instructions).toBe(
      "Always answer in English",
    );
  });

  it("merges overridden model parameters into the caller settings", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({ model_params: { temperature: 0.8 } }),
    });
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
    });

    await recorded.generate("caller input", {
      modelSettings: { maxOutputTokens: 500, temperature: 0.1, topP: 0.2 },
    });

    expect(agent.calls[0]?.options.modelSettings).toEqual({
      maxOutputTokens: 500,
      temperature: 0.8,
      topP: 0.2,
    });
    const llm = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "llm_call");
    expect(llm?.model_params).toEqual({
      maxOutputTokens: 500,
      temperature: 0.8,
      topP: 0.2,
    });
  });

  it("rejects out-of-range model parameters before creating a session", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({ model_params: { maxOutputTokens: -5 } }),
    });
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
    });

    await expect(recorded.generate("caller input")).rejects.toThrow(
      "maxOutputTokens must be an integer between 1 and 1000000",
    );

    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("rejects an unknown model parameter before creating a session", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({ model_params: { temprature: 0.4 } }),
    });
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
    });

    await expect(recorded.generate("caller input")).rejects.toThrow(
      "Unsupported replay model setting 'temprature'",
    );

    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("rejects a replacement model outside allowedReplayModels", async () => {
    vi.stubEnv("KITARU_OVERRIDE", JSON.stringify({ model: "o3-pro" }));
    const api = installTestApi();
    const resolver = vi.fn(async (modelId: string) => modelId);
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      allowedReplayModels: ["replacement-model"],
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
      resolveModel: resolver,
    });

    await expect(recorded.generate("caller input")).rejects.toThrow(
      "Replacement model 'o3-pro' is not in allowedReplayModels",
    );

    expect(resolver).not.toHaveBeenCalled();
    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("keeps the requested model when an exact model-map entry is absent", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: replaySpec({ model: { other: "replacement-model" } }),
    });
    const resolver = vi.fn(async (modelId: string) => modelId);
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      allowedReplayModels: ["replacement-model"],
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
      resolveModel: resolver,
    });

    await recorded.generate("caller input", { model: "caller-model" });

    expect(resolver).not.toHaveBeenCalled();
    expect(agent.calls[0]?.options.model).toBe("caller-model");
  });

  it("rejects invalid model-map values before creating a session", async () => {
    vi.stubEnv(
      "KITARU_OVERRIDE",
      JSON.stringify({ model: { "fallback-model": 42 } }),
    );
    const api = installTestApi();
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      allowedReplayModels: ["replacement-model"],
      requestedModelId: "fallback-model",
      resolveModel: vi.fn(async (modelId: string) => modelId),
    });

    await expect(recorded.generate("caller input")).rejects.toThrow(
      "model values must be strings",
    );

    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("rejects unresolved replacement models before creating a session", async () => {
    vi.stubEnv(
      "KITARU_OVERRIDE",
      JSON.stringify({ model: "replacement-model" }),
    );
    const api = installTestApi();
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      allowedReplayModels: ["replacement-model"],
      apiUrl: "https://api.example",
      requestedModelId: "fallback-model",
    });

    await expect(recorded.generate("caller input")).rejects.toThrow(
      "without resolveModel",
    );

    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });
});
