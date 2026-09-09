import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

const schema = z.object({ answer: z.string() });
type ModelCall = { prompt: unknown; temperature?: number };

function makeModel(
  name: string,
  options: { error?: boolean; output?: string; omitFinish?: boolean } = {},
) {
  const calls: ModelCall[] = [];
  const model = new MastraLanguageModelV2Mock({
    modelId: name,
    provider: "test-provider",
    doGenerate: async (call: ModelCall) => {
      calls.push(call);
      return {
        content: [{ type: "text", text: "The answer is yes." }],
        finishReason: "stop",
        response: { id: `${name}-response`, modelId: `${name}-served` },
        usage: { inputTokens: 5, outputTokens: 2, totalTokens: 7 },
        warnings: [],
      };
    },
    doStream: async (call: ModelCall) => {
      calls.push(call);
      if (options.error) throw new Error("Structuring provider failed");

      return {
        stream: new ReadableStream({
          start(controller) {
            for (const chunk of [
              { type: "stream-start" as const, warnings: [] },
              {
                type: "response-metadata" as const,
                id: `${name}-response`,
                modelId: `${name}-served`,
              },
              { type: "text-start" as const, id: "answer" },
              {
                type: "text-delta" as const,
                id: "answer",
                delta: options.output ?? '{"answer":"yes"}',
              },
              { type: "text-end" as const, id: "answer" },
              {
                type: "finish" as const,
                finishReason: "stop" as const,
                usage: { inputTokens: 11, outputTokens: 4, totalTokens: 15 },
              },
            ]) {
              if (options.omitFinish && chunk.type === "finish") continue;
              controller.enqueue(chunk);
            }
            controller.close();
          },
        }),
      };
    },
  });
  return { calls, model };
}

function wrap<T extends { generate: (...args: never[]) => unknown }>(agent: T) {
  return new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "parent",
  });
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("secondary structured-output model recording", () => {
  it("records both model calls and preserves native output", async () => {
    const api = installTestApi();
    const parent = makeModel("parent");
    const secondary = makeModel("secondary");
    const originalGenerate = secondary.model.doGenerate;
    const originalStream = secondary.model.doStream;
    const structuredOutput = { schema, model: secondary.model };
    const agent = new Agent({
      id: "structured",
      name: "structured",
      instructions: "Answer briefly",
      model: parent.model,
    });

    const result = await wrap(agent).generate("Should I proceed?", {
      structuredOutput,
    });

    expect(result.object).toEqual({ answer: "yes" });
    expect(parent.calls).toHaveLength(1);
    expect(secondary.calls).toHaveLength(1);
    const nodes = api
      .nodeBatches()
      .flat()
      .filter((node) => node.node_type === "llm_call");
    expect(nodes).toHaveLength(2);
    expect(nodes.find((node) => node.model === "parent-served")).toMatchObject({
      requested_model: "parent",
      tokens: { input_tokens: 5, output_tokens: 2 },
    });
    const child = nodes.find((node) => node.model === "secondary-served");
    expect(child).toMatchObject({
      requested_model: "secondary",
      status: "completed",
      tokens: { input_tokens: 11, output_tokens: 4 },
    });
    expect(JSON.stringify(child?.inputs)).toContain("The answer is yes.");
    expect(JSON.stringify(child?.outputs)).toContain("yes");
    expect(api.nodeBatches().at(-1)?.[0]).toMatchObject({
      outputs: { object: { answer: "yes" } },
    });
    expect(secondary.model.doGenerate).toBe(originalGenerate);
    expect(secondary.model.doStream).toBe(originalStream);
  });

  it("rejects a default secondary model before execution or recording", async () => {
    const api = installTestApi();
    const parent = makeModel("parent");
    const secondary = makeModel("secondary");
    const agent = wrap(
      new Agent({
        id: "defaults",
        name: "defaults",
        instructions: "Answer",
        model: parent.model,
        defaultOptions: {
          structuredOutput: { schema, model: secondary.model },
        } as never,
      }),
    );
    await expect(agent.generate("question")).rejects.toThrow(
      "structuredOutput.model",
    );
    expect(parent.calls).toHaveLength(0);
    expect(secondary.calls).toHaveLength(0);
    expect(api.calls).toHaveLength(0);
  });

  it.each([
    [{ useAgent: true }, {}],
    [{ errorStrategy: "warn" }, {}],
    [{ useAgent: true }, { useAgent: undefined }],
    [{ errorStrategy: "warn" }, { errorStrategy: undefined }],
  ])("rejects unsupported structuring options inherited from defaults: %s", async (defaults, callerOptions) => {
    const api = installTestApi();
    const parent = makeModel("parent");
    const secondary = makeModel("secondary");
    const agent = wrap(
      new Agent({
        id: "inherited",
        name: "inherited",
        instructions: "Answer",
        model: parent.model,
        defaultOptions: { structuredOutput: { schema, ...defaults } } as never,
      }),
    );
    await expect(
      agent.generate("question", {
        structuredOutput: { schema, model: secondary.model, ...callerOptions },
      }),
    ).rejects.toThrow();
    expect(parent.calls).toHaveLength(0);
    expect(secondary.calls).toHaveLength(0);
    expect(api.calls).toHaveLength(0);
  });

  it("preserves native output but fails recording when the secondary stream ends without a finish event", async () => {
    const api = installTestApi();
    const secondary = makeModel("secondary", { omitFinish: true });
    const agent = wrap(
      new Agent({
        id: "incomplete",
        name: "incomplete",
        instructions: "Answer",
        model: makeModel("parent").model,
      }),
    );
    const result = await agent.generate("question", {
      structuredOutput: { schema, model: secondary.model },
    });
    expect(result.object).toEqual({ answer: "yes" });
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
    ).toMatchObject({ status: "failed" });
    expect(
      api
        .nodeBatches()
        .flat()
        .find((node) => node.model === "secondary-served"),
    ).toMatchObject({
      status: "failed",
      error: expect.stringContaining("finish event"),
    });
  });

  it("fails the session when a successful provider response fails schema validation", async () => {
    const api = installTestApi();
    const secondary = makeModel("secondary", { output: '{"answer":42}' });
    const agent = wrap(
      new Agent({
        id: "validation",
        name: "validation",
        instructions: "Answer",
        model: makeModel("parent").model,
      }),
    );
    const result = await agent.generate("question", {
      structuredOutput: { schema, model: secondary.model },
    });
    expect(result.tripwire).toBeDefined();
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
    ).toMatchObject({ status: "failed" });
    expect(
      api
        .nodeBatches()
        .flat()
        .find((node) => node.model === "secondary-served"),
    ).toMatchObject({ status: "completed" });
  });

  it.each([
    "warn",
    "fallback",
  ] as const)("rejects secondary errorStrategy %s before execution", async (errorStrategy) => {
    const api = installTestApi();
    const parent = makeModel("parent");
    const secondary = makeModel("secondary");
    const agent = wrap(
      new Agent({
        id: "strategy",
        name: "strategy",
        instructions: "Answer",
        model: parent.model,
      }),
    );
    const structuredOutput =
      errorStrategy === "fallback"
        ? {
            schema,
            model: secondary.model,
            errorStrategy,
            fallbackValue: { answer: "unavailable" },
          }
        : { schema, model: secondary.model, errorStrategy };
    await expect(
      agent.generate("question", { structuredOutput }),
    ).rejects.toThrow("errorStrategy");
    expect(parent.calls).toHaveLength(0);
    expect(secondary.calls).toHaveLength(0);
    expect(api.calls).toHaveLength(0);
  });

  it("keeps recording isolated when invocations share the same secondary model", async () => {
    const api = installTestApi();
    const secondary = makeModel("secondary");
    const agent = wrap(
      new Agent({
        id: "concurrent",
        name: "concurrent",
        instructions: "Answer",
        model: makeModel("parent").model,
      }),
    );
    const results = await Promise.all(
      ["first", "second"].map((prompt) =>
        agent.generate(prompt, {
          structuredOutput: { schema, model: secondary.model },
        }),
      ),
    );
    expect(results.map((result) => result.object)).toEqual([
      { answer: "yes" },
      { answer: "yes" },
    ]);
    expect(api.sessionIds).toHaveLength(2);
    for (const sessionId of api.sessionIds) {
      const nodes = api
        .nodeBatches(sessionId)
        .flat()
        .filter((node) => node.node_type === "llm_call");
      expect(nodes).toHaveLength(2);
      expect(
        nodes.filter((node) => node.requested_model === "secondary"),
      ).toHaveLength(1);
    }
  });

  it("records a secondary provider failure and fails the session", async () => {
    const api = installTestApi();
    const secondary = makeModel("secondary", { error: true });
    const agent = wrap(
      new Agent({
        id: "failure",
        name: "failure",
        instructions: "Answer",
        model: makeModel("parent").model,
      }),
    );
    const result = await agent.generate("question", {
      structuredOutput: {
        schema,
        model: secondary.model,
        errorStrategy: "strict",
      },
    });
    expect(result.tripwire).toBeDefined();
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
    ).toMatchObject({ status: "failed" });
    const nodes = api
      .nodeBatches()
      .flat()
      .filter(
        (node) =>
          node.node_type === "llm_call" && node.requested_model === "secondary",
      );
    expect(nodes.length).toBeGreaterThan(0);
    expect(nodes).toHaveLength(secondary.calls.length);
    expect(nodes.every((node) => node.status === "failed")).toBe(true);
    expect(JSON.stringify(nodes)).toContain("Structuring provider failed");
  });

  it("applies replay model overrides to the parent and retains the secondary model", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: {
        baseline_session_id: ORIGINAL_SESSION_ID,
        id: REPLAY_ID,
        override: {
          model: { parent: "replacement" },
          model_params: { temperature: 0.7 },
        },
        status: "pending",
        tool_policy: { default: { type: "passthrough" }, tools: {} },
      },
    });
    const parent = makeModel("parent");
    const replacement = makeModel("replacement");
    const secondary = makeModel("secondary");
    const agent = new KitaruAgent(
      new Agent({
        id: "replay",
        name: "replay",
        instructions: "Answer",
        model: parent.model,
      }),
      {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "parent",
        allowedReplayModels: ["replacement"],
        resolveModel: () => replacement.model,
      },
    );
    const result = await agent.generate("question", {
      structuredOutput: { schema, model: secondary.model },
    });
    expect(result.object).toEqual({ answer: "yes" });
    expect(parent.calls).toHaveLength(0);
    expect(replacement.calls).toHaveLength(1);
    expect(replacement.calls[0]?.temperature).toBe(0.7);
    expect(secondary.calls).toHaveLength(1);
    expect(secondary.calls[0]?.temperature).not.toBe(0.7);
    expect(
      api
        .nodeBatches()
        .flat()
        .find((node) => node.model === "secondary-served"),
    ).toMatchObject({ requested_model: "secondary" });
  });

  it("rejects parent-agent structuring before execution or API writes", async () => {
    const api = installTestApi();
    const parent = makeModel("parent");
    const secondary = makeModel("secondary");
    const agent = wrap(
      new Agent({
        id: "unsupported",
        name: "unsupported",
        instructions: "Answer",
        model: parent.model,
      }),
    );
    await expect(
      agent.generate("question", {
        structuredOutput: { schema, model: secondary.model, useAgent: true },
      }),
    ).rejects.toThrow("useAgent");
    expect(parent.calls).toHaveLength(0);
    expect(secondary.calls).toHaveLength(0);
    expect(api.calls).toHaveLength(0);
  });
});
