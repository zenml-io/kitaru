import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { AGENT_ID, FakeClient, TEST_USAGE, toolResponse } from "./helpers.js";

const EMPTY_INPUT = jsonSchema<Record<string, never>>({
  additionalProperties: false,
  properties: {},
  type: "object",
});

describe("adapter failures", () => {
  it("records a failed model call after the provider invocation starts", async () => {
    const client = new FakeClient();
    const providerError = new Error("provider unavailable");
    const onStart = vi.fn();
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw providerError;
      },
      modelId: "failed-model",
      provider: "failed-provider.v4",
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await expect(
      generate({ model, onLanguageModelCallStart: onStart, prompt: "go" }),
    ).rejects.toBe(providerError);

    expect(onStart).toHaveBeenCalledOnce();
    expect(model.doGenerateCalls).toHaveLength(1);
    const llmNodes = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .filter((node) => node.node_type === "llm_call");
    expect(llmNodes).toHaveLength(1);
    expect(llmNodes).toContainEqual(
      expect.objectContaining({
        error: "provider unavailable",
        model: "failed-model",
        model_provider: "failed-provider",
        node_type: "llm_call",
        status: "failed",
      }),
    );
    expect(client.updated.at(-1)?.status).toBe("failed");
  });

  it("stops the next model call when step recording fails", async () => {
    const client = new FakeClient({
      failNodeBatch: (batch) =>
        batch.nodes.some((node) => node.node_type === "llm_call"),
    });
    const model = new MockLanguageModelV4({
      doGenerate: [
        toolResponse([{ id: "call-1", name: "work" }]),
        {
          content: [{ text: "must not run", type: "text" }],
          finishReason: { raw: "stop", unified: "stop" },
          usage: TEST_USAGE,
          warnings: [],
        },
      ],
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await expect(
      generate({
        model,
        prompt: "go",
        stopWhen: () => false,
        tools: {
          work: tool({ execute: async () => "done", inputSchema: EMPTY_INPUT }),
        },
      }),
    ).rejects.toThrow("node upload failed");
    expect(model.doGenerateCalls).toHaveLength(1);
  });

  it("keeps the first Kitaru failure when cleanup also fails", async () => {
    const client = new FakeClient({
      failNodeBatch: (batch) =>
        batch.nodes.some((node) => node.node_type === "llm_call"),
      updateFails: true,
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await expect(
      generate({
        model: new MockLanguageModelV4({
          doGenerate: {
            content: [{ text: "done", type: "text" }],
            finishReason: { raw: "stop", unified: "stop" },
            usage: TEST_USAGE,
            warnings: [],
          },
        }),
        prompt: "go",
      }),
    ).rejects.toThrow("node upload failed");
  });

  it("does not retry or roll back a completed passthrough side effect", async () => {
    const client = new FakeClient({
      failNodeBatch: (batch) =>
        batch.nodes.some((node) => node.node_type === "llm_call"),
    });
    let mutations = 0;
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await expect(
      generate({
        maxRetries: 9,
        model: new MockLanguageModelV4({
          doGenerate: toolResponse([{ id: "call-1", name: "mutate" }]),
        }),
        prompt: "go",
        tools: {
          mutate: tool({
            execute: async () => {
              mutations += 1;
              return "committed";
            },
            inputSchema: EMPTY_INPUT,
          }),
        },
      }),
    ).rejects.toThrow("node upload failed");
    expect(mutations).toBe(1);
  });
});
