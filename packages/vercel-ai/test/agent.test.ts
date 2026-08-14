import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it } from "vitest";

import { createKitaruToolLoopAgent } from "../src/index.js";
import { AGENT_ID, FakeClient, textResponse, toolResponse } from "./helpers.js";

describe("createKitaruToolLoopAgent", () => {
  it("records generate calls and returns the native result", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: textResponse("recorded"),
    });
    const agent = createKitaruToolLoopAgent(
      { id: "support-agent", model },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.generate({ prompt: "Help" });

    expect(result.text).toBe("recorded");
    expect(Object.getPrototypeOf(result)).not.toBe(Object.prototype);
    expect(agent.version).toBe("agent-v1");
    expect(agent.id).toBe("support-agent");
    expect(client.created).toHaveLength(1);
    expect(client.updated.at(-1)?.status).toBe("completed");
  });

  it("preserves the native tools value when no tools are configured", () => {
    const model = new MockLanguageModelV4();
    const agent = createKitaruToolLoopAgent(
      { model },
      { agentId: AGENT_ID, client: new FakeClient(), environment: {} },
    );

    expect(agent.tools).toBeUndefined();
  });

  it("records model settings resolved by prepareStep", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: [
        toolResponse([{ id: "call-1", name: "work" }]),
        textResponse("done"),
      ],
    });
    const agent = createKitaruToolLoopAgent(
      {
        model,
        prepareStep: ({ stepNumber }) => ({
          temperature: stepNumber === 0 ? 0.2 : 0.7,
        }),
        tools: {
          work: tool({
            execute: async () => "done",
            inputSchema: jsonSchema<Record<never, never>>({
              additionalProperties: false,
              properties: {},
              type: "object",
            }),
          }),
        },
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    await agent.generate({ prompt: "Work" });

    const modelNodes = client.nodeBatches.flatMap((batch) =>
      batch.nodes.filter((node) => node.node_type === "llm_call"),
    );
    expect(modelNodes.map((node) => node.model_params)).toEqual([
      { temperature: 0.2 },
      { temperature: 0.7 },
    ]);
  });

  it("preserves provider error identity and fails the Kitaru session", async () => {
    const client = new FakeClient();
    const providerError = new Error("provider failed");
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw providerError;
      },
    });
    const agent = createKitaruToolLoopAgent(
      { maxRetries: 0, model },
      { agentId: AGENT_ID, client, environment: {} },
    );

    await expect(agent.generate({ prompt: "Help" })).rejects.toBe(
      providerError,
    );
    expect(client.updated.at(-1)).toMatchObject({
      error: "provider failed",
      status: "failed",
    });
    expect(
      client.nodeBatches
        .flatMap((batch) => batch.nodes)
        .find((node) => node.node_type === "llm_call"),
    ).toMatchObject({ error: "provider failed", status: "failed" });
  });

  it("records the effective prepareCall model and preserves callbacks", async () => {
    const client = new FakeClient();
    const events: string[] = [];
    const originalModel = new MockLanguageModelV4();
    const preparedModel = new MockLanguageModelV4({
      doGenerate: textResponse("prepared"),
    });
    const agent = createKitaruToolLoopAgent(
      {
        callOptionsSchema: jsonSchema<{ tenant: string }>({
          additionalProperties: false,
          properties: { tenant: { type: "string" } },
          required: ["tenant"],
          type: "object",
        }),
        model: originalModel,
        onEnd: () => {
          events.push("configured:end");
        },
        prepareCall: ({ options, ...call }) => ({
          ...call,
          model: preparedModel,
          prompt: `Help ${options.tenant}`,
        }),
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.generate({
      onEnd: () => {
        events.push("call:end");
      },
      options: { tenant: "acme" },
      prompt: "original",
    });

    expect(result.text).toBe("prepared");
    expect(originalModel.doGenerateCalls).toHaveLength(0);
    expect(preparedModel.doGenerateCalls).toHaveLength(1);
    expect(events).toEqual(["configured:end", "call:end"]);
    expect(client.created[0]?.inputs).toBe("Help acme");
  });

  it("keeps recorder state separate for concurrent generate calls", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doGenerate: [textResponse("first"), textResponse("second")],
    });
    const agent = createKitaruToolLoopAgent(
      { model },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const [first, second] = await Promise.all([
      agent.generate({ prompt: "one" }),
      agent.generate({ prompt: "two" }),
    ]);

    expect(new Set([first.text, second.text])).toEqual(
      new Set(["first", "second"]),
    );
    expect(client.created).toHaveLength(2);
    expect(
      client.updated.filter((update) => update.status === "completed"),
    ).toHaveLength(2);
  });

  it("delegates stream calls without creating a Kitaru session", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({
      doStream: {
        stream: new ReadableStream({
          start(controller) {
            controller.enqueue({ id: "text-1", type: "text-start" });
            controller.enqueue({
              delta: "streamed",
              id: "text-1",
              type: "text-delta",
            });
            controller.enqueue({ id: "text-1", type: "text-end" });
            controller.enqueue({
              finishReason: { raw: "stop", unified: "stop" },
              type: "finish",
              usage: {
                inputTokens: {
                  cacheRead: undefined,
                  cacheWrite: undefined,
                  noCache: 1,
                  total: 1,
                },
                outputTokens: {
                  reasoning: undefined,
                  text: 1,
                  total: 1,
                },
              },
            });
            controller.close();
          },
        }),
      },
    });
    const agent = createKitaruToolLoopAgent(
      { model },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.stream({ prompt: "Help" });

    await expect(result.text).resolves.toBe("streamed");
    expect(client.created).toHaveLength(0);
    expect(client.replayReads).toBe(0);
  });
});
