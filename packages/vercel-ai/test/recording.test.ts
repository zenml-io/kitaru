import type { SessionNodeCreateRequest } from "@zenml-io/kitaru";
import {
  MAX_RECORDED_PAYLOAD_CHARS,
  MAX_RECORDED_STRING_CHARS,
} from "@zenml-io/kitaru/adapter";
import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { AGENT_ID, FakeClient, textResponse, toolResponse } from "./helpers.js";

const LONG_TEXT = "y".repeat(MAX_RECORDED_STRING_CHARS + 904);
const VALUE_INPUT = jsonSchema<{ value: string }>({
  additionalProperties: false,
  properties: { value: { type: "string" } },
  required: ["value"],
  type: "object",
});

function nodesOf(client: FakeClient): SessionNodeCreateRequest[] {
  return client.nodeBatches.flatMap((batch) => batch.nodes);
}

function llmNode(client: FakeClient): SessionNodeCreateRequest | undefined {
  return nodesOf(client).find((node) => node.node_type === "llm_call");
}

describe("recording large payloads", () => {
  it("records model text longer than the metadata bound without failing the run", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: textResponse(LONG_TEXT),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    const result = await generate({ model, prompt: "go" });

    expect(result.text).toBe(LONG_TEXT);
    expect(client.updated.at(-1)?.status).toBe("completed");
    expect(llmNode(client)?.outputs).toMatchObject({ text: LONG_TEXT });
  });

  it("records long tool arguments and results without failing the run", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: toolResponse([
        {
          id: "call-1",
          input: JSON.stringify({ value: LONG_TEXT }),
          name: "write",
        },
      ]),
    });
    const execute = vi.fn(async () => LONG_TEXT);
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    const result = await generate({
      model,
      prompt: "go",
      tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
    });

    expect(execute).toHaveBeenCalledOnce();
    expect(result.toolResults[0]?.output).toBe(LONG_TEXT);
    expect(client.updated.at(-1)?.status).toBe("completed");
    const toolNode = nodesOf(client).find(
      (node) => node.node_type === "tool_call",
    );
    expect(toolNode?.inputs).toEqual({ value: LONG_TEXT });
    expect(toolNode?.outputs).toBe(LONG_TEXT);
  });

  it("degrades a payload beyond the recording ceiling instead of failing the run", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: textResponse("z".repeat(MAX_RECORDED_PAYLOAD_CHARS + 1)),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({ model, prompt: "go" });

    expect(client.updated.at(-1)?.status).toBe("completed");
    expect(llmNode(client)?.outputs).toMatchObject({
      kitaru_recording: "degraded",
    });
  });
});

describe("recorded model identity", () => {
  it("records the served model id and the requested Kitaru model id", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: {
        ...textResponse(),
        response: {
          id: "response-1",
          modelId: "gpt-5-nano-2026-08-07",
          timestamp: new Date(0),
        },
      },
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
      requestedModelId: "openai/gpt-5-nano",
    });

    await generate({ model, prompt: "go" });

    expect(llmNode(client)).toMatchObject({
      model: "gpt-5-nano-2026-08-07",
      requested_model: "openai/gpt-5-nano",
    });
  });

  it("records the bare provider family and keeps the qualified provider id", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: textResponse(),
      provider: "openai.responses",
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({ model, prompt: "go" });

    expect(llmNode(client)?.model_provider).toBe("openai");
    expect(llmNode(client)?.attributes).toMatchObject({
      provider_id: "openai.responses",
    });
  });
});

describe("recorded cost", () => {
  it("records a calculated cost and marks it estimated", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const costCalculator = vi.fn(() => 0.000125);
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      costCalculator,
      environment: {},
      requestedModelId: "openai/gpt-5-nano",
    });

    await generate({ model, prompt: "go" });

    expect(costCalculator).toHaveBeenCalledWith({
      model: "mock-model-id",
      provider: "mock-provider",
      requestedModelId: "openai/gpt-5-nano",
      tokens: { input_tokens: 3, output_tokens: 2 },
    });
    expect(llmNode(client)?.cost).toBe(0.000125);
    expect(llmNode(client)?.attributes).toMatchObject({
      cost: { source: "user", status: "estimated" },
    });
  });

  it("marks cost disabled when no calculator is configured", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await generate({ model, prompt: "go" });

    expect(llmNode(client)?.cost).toBeNull();
    expect(llmNode(client)?.attributes).toMatchObject({
      cost: { source: "none", status: "disabled" },
    });
  });

  it("keeps the run alive when the cost calculator throws", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      costCalculator: () => {
        throw new RangeError("no price for this model");
      },
      environment: {},
    });

    await generate({ model, prompt: "go" });

    expect(client.updated.at(-1)?.status).toBe("completed");
    expect(llmNode(client)?.cost).toBeNull();
    expect(llmNode(client)?.attributes).toMatchObject({
      cost: { error_type: "RangeError", source: "user", status: "unavailable" },
    });
  });
});
