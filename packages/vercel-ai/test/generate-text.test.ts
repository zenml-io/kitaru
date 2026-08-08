import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { AGENT_ID, FakeClient, textResponse, toolResponse } from "./helpers.js";

const EMPTY_INPUT = jsonSchema<Record<string, never>>({
  additionalProperties: false,
  properties: {},
  type: "object",
});

describe("createKitaruGenerateText", () => {
  it("returns the native result object and records a no-tool call safely", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: textResponse("hello"),
    });
    const onStepEnd = vi.fn();
    const kitaruGenerateText = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    const result = await kitaruGenerateText({
      maxRetries: 2,
      model,
      onStepEnd,
      prompt: "private prompt",
    });

    expect(result.text).toBe("hello");
    expect(Object.getPrototypeOf(result)).not.toBe(Object.prototype);
    expect(onStepEnd).toHaveBeenCalledOnce();
    expect(model.doGenerateCalls).toHaveLength(1);
    expect(client.created[0]?.inputs).toBe("private prompt");
    const llmNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "llm_call");
    expect(llmNode).toMatchObject({
      inputs: null,
      model: "mock-model-id",
      model_provider: "mock-provider",
      status: "completed",
    });
    expect(llmNode?.attributes).toMatchObject({ provider_metadata: null });
  });

  it("records bounded provider metadata without the whole SDK result", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: {
        ...textResponse(),
        providerMetadata: {
          fixture: { requestId: "safe-id", secret: "must-not-record" },
        },
      },
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({ model, prompt: "go" });

    const llmNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "llm_call");
    expect(llmNode?.attributes).toMatchObject({
      provider_metadata: {
        fixture: { requestId: "safe-id", secret: "[redacted]" },
      },
    });
  });

  it("preserves native baseline tool concurrency", async () => {
    const client = new FakeClient();
    const events: string[] = [];
    const model = new MockLanguageModelV4({
      doGenerate: toolResponse([
        { id: "call-1", name: "first" },
        { id: "call-2", name: "second" },
      ]),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({
      model,
      prompt: "go",
      tools: {
        first: tool({
          execute: async (): Promise<string> => {
            events.push("first:start");
            await Promise.resolve();
            events.push("first:end");
            return "first";
          },
          inputSchema: EMPTY_INPUT,
        }),
        second: tool({
          execute: async () => {
            events.push("second:start", "second:end");
            return "second";
          },
          inputSchema: EMPTY_INPUT,
        }),
      },
    });

    expect(events).toEqual([
      "first:start",
      "second:start",
      "second:end",
      "first:end",
    ]);
  });

  it("keeps an ordinary application tool error as a native tool-error", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: toolResponse([{ id: "call-1", name: "fails" }]),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    const result = await generate({
      model,
      prompt: "go",
      tools: {
        fails: tool({
          execute: async (): Promise<string> => {
            throw new Error("application failed");
          },
          inputSchema: EMPTY_INPUT,
        }),
      },
    });

    expect(result.content.some((part) => part.type === "tool-error")).toBe(
      true,
    );
    expect(client.updated.at(-1)?.status).toBe("completed");
    const failedTool = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "tool_call");
    expect(failedTool).toMatchObject({
      error: "application failed",
      status: "failed",
    });
  });

  it("preserves native async-iterable tools during baseline recording", async () => {
    const client = new FakeClient();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    const result = await generate({
      model: new MockLanguageModelV4({
        doGenerate: toolResponse([{ id: "call-1", name: "streamingTool" }]),
      }),
      prompt: "go",
      tools: {
        streamingTool: tool({
          execute: async function* () {
            yield "partial";
            yield "final";
          },
          inputSchema: EMPTY_INPUT,
        }),
      },
    });

    expect(result.toolResults[0]?.output).toBe("final");
    expect(client.updated.at(-1)?.status).toBe("completed");
  });
});
