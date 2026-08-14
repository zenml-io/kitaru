import { jsonSchema, Output, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruToolLoopAgent } from "../src/index.js";
import {
  AGENT_ID,
  FakeClient,
  replayEnvironment,
  replaySpec,
  textResponse,
  toolResponse,
} from "./helpers.js";

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

  it("records caller model settings without applying replay bounds", async () => {
    const client = new FakeClient();
    const onLanguageModelCallStart = vi.fn();
    const agent = createKitaruToolLoopAgent(
      {
        model: new MockLanguageModelV4({ doGenerate: textResponse("done") }),
        prepareCall: (call) => ({
          ...call,
          onLanguageModelCallStart,
        }),
        prepareStep: () => ({ temperature: 3, topK: 0 }),
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    await agent.generate({ prompt: "Help" });

    expect(onLanguageModelCallStart).toHaveBeenCalledOnce();
    expect(
      client.nodeBatches
        .flatMap((batch) => batch.nodes)
        .find((node) => node.node_type === "llm_call"),
    ).toMatchObject({ model_params: { temperature: 3, topK: 0 } });
    expect(client.updated.at(-1)?.status).toBe("completed");
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

  it("records provider failure when the start callback throws", async () => {
    const client = new FakeClient();
    const providerError = new Error("provider failed");
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw providerError;
      },
    });
    const agent = createKitaruToolLoopAgent(
      {
        maxRetries: 0,
        model,
        prepareCall: (call) => ({
          ...call,
          onLanguageModelCallStart: () => {
            throw new Error("callback failed");
          },
        }),
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    await expect(agent.generate({ prompt: "Help" })).rejects.toBe(
      providerError,
    );
    expect(
      client.nodeBatches
        .flatMap((batch) => batch.nodes)
        .find((node) => node.node_type === "llm_call"),
    ).toMatchObject({ error: "provider failed", status: "failed" });
  });

  it("records the model call when a post-response tool callback throws", async () => {
    const client = new FakeClient();
    const callbackError = new Error("input callback failed");
    const agent = createKitaruToolLoopAgent(
      {
        model: new MockLanguageModelV4({
          doGenerate: toolResponse([{ id: "call-1", name: "work" }]),
        }),
        tools: {
          work: tool({
            execute: async () => "done",
            inputSchema: jsonSchema<Record<never, never>>({
              additionalProperties: false,
              properties: {},
              type: "object",
            }),
            onInputStart: () => {
              throw callbackError;
            },
          }),
        },
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    await expect(agent.generate({ prompt: "Work" })).rejects.toBe(
      callbackError,
    );
    expect(
      client.nodeBatches
        .flatMap((batch) => batch.nodes)
        .find((node) => node.node_type === "llm_call"),
    ).toMatchObject({ error: "input callback failed", status: "failed" });
    expect(client.updated.at(-1)).toMatchObject({
      error: "input callback failed",
      status: "failed",
    });
  });

  it("stops before another model call after step recording fails", async () => {
    const client = new FakeClient({
      failNodeBatch: (_batch, index) => index === 1,
    });
    const model = new MockLanguageModelV4({
      doGenerate: [
        toolResponse([{ id: "call-1", name: "work" }]),
        textResponse("should not run"),
      ],
    });
    const agent = createKitaruToolLoopAgent(
      {
        model,
        stopWhen: () => false,
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

    await expect(agent.generate({ prompt: "Work" })).rejects.toThrow(
      "node upload failed",
    );
    expect(model.doGenerateCalls).toHaveLength(1);
    expect(client.updated.at(-1)?.status).toBe("failed");
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

  it("uses the native call settings when prepareCall returns undefined", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse("done") });
    const agent = createKitaruToolLoopAgent(
      {
        model,
        prepareCall: (() => undefined) as never,
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.generate({ prompt: "Help" });

    expect(result.text).toBe("done");
    expect(model.doGenerateCalls).toHaveLength(1);
    expect(client.updated.at(-1)?.status).toBe("completed");
  });

  it("records structured output configured by prepareCall", async () => {
    const client = new FakeClient();
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
    const agent = createKitaruToolLoopAgent(
      {
        model: new MockLanguageModelV4({
          doGenerate: textResponse('{"answer":"yes"}'),
        }),
        prepareCall: (call) => ({
          ...call,
          output: Output.object({ schema: answerSchema }),
        }),
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.generate({ prompt: "Answer" });

    expect(result.output).toEqual({ answer: "yes" });
    expect(client.updated.at(-1)?.outputs).toMatchObject({
      object: { answer: "yes" },
    });
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

  it("rejects stream calls when replay is active", async () => {
    const client = new FakeClient({ replay: replaySpec() });
    const execute = vi.fn(async () => "live");
    const model = new MockLanguageModelV4();
    const agent = createKitaruToolLoopAgent(
      {
        model,
        tools: {
          work: tool({
            execute,
            inputSchema: jsonSchema<Record<never, never>>({
              additionalProperties: false,
              properties: {},
              type: "object",
            }),
          }),
        },
      },
      { agentId: AGENT_ID, client, environment: replayEnvironment() },
    );

    await expect(agent.stream({ prompt: "Help" })).rejects.toThrow(
      "Agent stream replay is not supported",
    );
    expect(client.created).toHaveLength(0);
    expect(client.replayReads).toBe(0);
    expect(model.doStreamCalls).toHaveLength(0);
    expect(execute).not.toHaveBeenCalled();
  });
});
