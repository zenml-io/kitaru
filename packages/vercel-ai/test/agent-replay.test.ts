import { jsonSchema, tool } from "ai";
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

const VALUE_INPUT = jsonSchema<{ value: string }>({
  additionalProperties: false,
  properties: { value: { type: "string" } },
  required: ["value"],
  type: "object",
});

describe("ToolLoopAgent replay safety", () => {
  it.each([
    ["prepareStep", { prepareStep: () => ({}) }, {}],
    [
      "toolApproval",
      { toolApproval: { write: async () => "approved" as const } },
      {},
    ],
    [
      "approval-gated tool",
      {
        tools: {
          write: tool({
            execute: vi.fn(async () => "live"),
            inputSchema: VALUE_INPUT,
            needsApproval: true,
          }),
        },
      },
      {},
    ],
    ["provider tool", { tools: { write: { type: "provider" } } }, {}],
    [
      "dynamic tool",
      {
        tools: {
          write: { execute: vi.fn(async () => "live"), type: "dynamic" },
        },
      },
      {},
    ],
    ["sandbox", {}, { experimental_sandbox: {} }],
    [
      "pre-supplied approval message",
      {},
      {
        messages: [
          {
            content: [
              { approvalId: "approval-1", type: "tool-approval-request" },
            ],
            role: "assistant",
          },
        ],
      },
    ],
  ])("rejects %s before provider or tool side effects", async (_name, settings, call) => {
    const client = new FakeClient({ replay: replaySpec() });
    const model = new MockLanguageModelV4({
      doGenerate: toolResponse([
        { id: "call-1", input: '{"value":"a"}', name: "write" },
      ]),
    });
    const execute = vi.fn(async () => "live");
    const agent = createKitaruToolLoopAgent(
      {
        model,
        tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
        ...settings,
      } as never,
      {
        agentId: AGENT_ID,
        client,
        environment: replayEnvironment(),
      },
    );

    await expect(
      agent.generate({ prompt: "go", ...call } as never),
    ).rejects.toThrow(/approval|dynamic|prepareStep|provider|sandbox/i);

    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
    expect(execute).not.toHaveBeenCalled();
  });

  it("applies replay overrides after prepareCall", async () => {
    const preparedModel = new MockLanguageModelV4({
      modelId: "prepared-model",
      doGenerate: textResponse("prepared"),
    });
    const replacementModel = new MockLanguageModelV4({
      modelId: "replacement-model",
      doGenerate: textResponse("replacement"),
    });
    const client = new FakeClient({
      replay: replaySpec(
        { type: "passthrough" },
        {
          model: "replacement-model",
          model_params: { temperature: 0.25 },
          prompt: "replay prompt",
        },
      ),
    });
    const agent = createKitaruToolLoopAgent(
      {
        model: new MockLanguageModelV4(),
        prepareCall: ({ options: _options, ...call }) => ({
          ...call,
          model: preparedModel,
          prompt: "prepared prompt",
        }),
      },
      {
        agentId: AGENT_ID,
        allowedReplayModels: ["replacement-model"],
        client,
        environment: replayEnvironment(),
        resolveModel: async () => replacementModel,
      },
    );

    const result = await agent.generate({ prompt: "caller prompt" });

    expect(result.text).toBe("replacement");
    expect(preparedModel.doGenerateCalls).toHaveLength(0);
    expect(replacementModel.doGenerateCalls[0]).toMatchObject({
      temperature: 0.25,
    });
    expect(replacementModel.doGenerateCalls[0]?.prompt).toEqual([
      expect.objectContaining({
        content: [{ text: "replay prompt", type: "text" }],
        role: "user",
      }),
    ]);
    expect(client.created[0]?.inputs).toBe("replay prompt");
  });

  it("replays a static local tool without executing it", async () => {
    const client = new FakeClient({
      replay: replaySpec({
        cases: [
          {
            match: { value: "a" },
            match_mode: "exact",
            result: { source: "static" },
          },
        ],
        on_miss: "fail",
        type: "static",
      }),
    });
    const execute = vi.fn(async () => ({ source: "live" }));
    const model = new MockLanguageModelV4({
      doGenerate: [
        toolResponse([{ id: "call-1", input: '{"value":"a"}', name: "write" }]),
        textResponse("done"),
      ],
    });
    const agent = createKitaruToolLoopAgent(
      {
        model,
        tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
      },
      {
        agentId: AGENT_ID,
        client,
        environment: replayEnvironment(),
      },
    );

    const result = await agent.generate({ prompt: "go" });

    expect(result.text).toBe("done");
    expect(result.toolResults[0]?.output).toEqual({ source: "static" });
    expect(execute).not.toHaveBeenCalled();
    expect(client.updated.at(-1)?.status).toBe("completed");
  });
});

describe("ToolLoopAgent manual approval", () => {
  it("returns the native result and marks the session failed", async () => {
    const client = new FakeClient();
    const execute = vi.fn(async () => "live");
    const agent = createKitaruToolLoopAgent(
      {
        model: new MockLanguageModelV4({
          doGenerate: toolResponse([
            { id: "call-1", input: '{"value":"a"}', name: "write" },
          ]),
        }),
        tools: {
          write: tool({
            execute,
            inputSchema: VALUE_INPUT,
            needsApproval: true,
          }),
        },
      },
      { agentId: AGENT_ID, client, environment: {} },
    );

    const result = await agent.generate({ prompt: "go" });

    expect(Object.getPrototypeOf(result)).not.toBe(Object.prototype);
    expect(result.content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "tool-approval-request" }),
      ]),
    );
    expect(execute).not.toHaveBeenCalled();
    expect(client.updated.at(-1)).toMatchObject({
      error: "manual_approval_continuation_unsupported",
      status: "failed",
    });
  });
});
