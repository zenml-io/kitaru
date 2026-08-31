import { jsonSchema, Output, tool } from "ai";
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

  it("preserves deprecated callbacks and gives stable names precedence", async () => {
    const client = new FakeClient();
    const stableStart = vi.fn();
    const deprecatedStart = vi.fn();
    const stableEnd = vi.fn();
    const deprecatedEnd = vi.fn();
    const stableStep = vi.fn();
    const deprecatedStep = vi.fn();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({
      experimental_onLanguageModelCallEnd: deprecatedEnd,
      experimental_onLanguageModelCallStart: deprecatedStart,
      model: new MockLanguageModelV4({ doGenerate: textResponse() }),
      onLanguageModelCallEnd: stableEnd,
      onLanguageModelCallStart: stableStart,
      onStepEnd: stableStep,
      onStepFinish: deprecatedStep,
      prompt: "go",
    });

    expect(stableStart).toHaveBeenCalledOnce();
    expect(stableEnd).toHaveBeenCalledOnce();
    expect(stableStep).toHaveBeenCalledOnce();
    expect(deprecatedStart).not.toHaveBeenCalled();
    expect(deprecatedEnd).not.toHaveBeenCalled();
    expect(deprecatedStep).not.toHaveBeenCalled();

    const aliasStart = vi.fn();
    const aliasEnd = vi.fn();
    const aliasStep = vi.fn();
    await generate({
      experimental_onLanguageModelCallEnd: aliasEnd,
      experimental_onLanguageModelCallStart: aliasStart,
      model: new MockLanguageModelV4({ doGenerate: textResponse() }),
      onStepFinish: aliasStep,
      prompt: "go",
    });

    expect(aliasStart).toHaveBeenCalledOnce();
    expect(aliasEnd).toHaveBeenCalledOnce();
    expect(aliasStep).toHaveBeenCalledOnce();
  });

  it("records configured structured output in the run summary", async () => {
    const client = new FakeClient();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });
    const answerSchema = jsonSchema<{ answer: string }>(
      {
        additionalProperties: false,
        properties: { answer: { type: "string" } },
        required: ["answer"],
        type: "object",
      },
      {
        validate: (value) =>
          typeof value === "object" &&
          value !== null &&
          typeof (value as { answer?: unknown }).answer === "string"
            ? { success: true, value: value as { answer: string } }
            : { success: false, error: new TypeError("invalid answer") },
      },
    );

    const result = await generate({
      model: new MockLanguageModelV4({
        doGenerate: textResponse('{"answer":"yes"}'),
      }),
      output: Output.object({ schema: answerSchema }),
      prompt: "go",
    });

    expect(result.output).toEqual({ answer: "yes" });
    expect(client.updated.at(-1)?.outputs).toMatchObject({
      object: { answer: "yes" },
    });
  });

  it("does not fail a non-stop generation whose structured output is unavailable", async () => {
    const client = new FakeClient();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });
    const answerSchema = jsonSchema<{ answer: string }>({
      additionalProperties: false,
      properties: { answer: { type: "string" } },
      required: ["answer"],
      type: "object",
    });

    const result = await generate({
      model: new MockLanguageModelV4({
        doGenerate: {
          ...textResponse(),
          content: [],
          finishReason: { raw: "length", unified: "length" },
        },
      }),
      output: Output.object({ schema: answerSchema }),
      prompt: "go",
    });

    expect(result.finishReason).toBe("length");
    expect(client.updated.at(-1)).toMatchObject({ status: "completed" });
    expect(client.updated.at(-1)?.outputs).not.toHaveProperty("object");
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

  it("records provider warnings", async () => {
    const client = new FakeClient();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({
      model: new MockLanguageModelV4({
        doGenerate: {
          ...textResponse(),
          warnings: [
            {
              details: "ignored",
              feature: "temperature",
              type: "unsupported",
            },
          ],
        },
      }),
      prompt: "go",
    });

    const llmNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "llm_call");
    expect(llmNode?.outputs).toMatchObject({
      warnings: [
        {
          details: "ignored",
          feature: "temperature",
          type: "unsupported",
        },
      ],
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
    const toolNodes = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .filter((node) => node.node_type === "tool_call");
    expect(toolNodes).toHaveLength(2);
    for (const node of toolNodes) {
      expect(typeof node.started_at).toBe("string");
      expect(Date.parse(node.started_at ?? "")).toBeLessThanOrEqual(
        Date.parse(node.ended_at ?? ""),
      );
    }
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
