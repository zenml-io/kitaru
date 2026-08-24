import { ToolPolicyError } from "@zenml-io/kitaru";
import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import {
  AGENT_ID,
  FakeClient,
  replayEnvironment,
  replaySpec,
  TEST_USAGE,
  toolResponse,
} from "./helpers.js";

const VALUE_INPUT = jsonSchema<{ value: string }>(
  {
    additionalProperties: false,
    properties: { value: { type: "string" } },
    required: ["value"],
    type: "object",
  },
  {
    validate: (value) =>
      typeof value === "object" &&
      value !== null &&
      typeof (value as { value?: unknown }).value === "string"
        ? { success: true, value: value as { value: string } }
        : { success: false, error: new TypeError("invalid value") },
  },
);
const SECRET_INPUT = jsonSchema<{ authorization: string }>({
  additionalProperties: false,
  properties: { authorization: { type: "string" } },
  required: ["authorization"],
  type: "object",
});

const SUCCESS_OUTPUT = jsonSchema<{ saved: boolean }>(
  {
    additionalProperties: false,
    properties: { saved: { type: "boolean" } },
    required: ["saved"],
    type: "object",
  },
  {
    validate: (value) =>
      typeof value === "object" &&
      value !== null &&
      typeof (value as { saved?: unknown }).saved === "boolean"
        ? { success: true, value: value as { saved: boolean } }
        : { success: false, error: new TypeError("invalid saved result") },
  },
);

function modelForValue() {
  return new MockLanguageModelV4({
    doGenerate: toolResponse([
      { id: "call-1", input: '{"value":"a"}', name: "write" },
    ]),
  });
}

describe("replay tool policies", () => {
  it("does not look up history with redacted credentials", async () => {
    const client = new FakeClient({
      replay: replaySpec({ on_miss: "fail", type: "history" }),
    });
    const execute = vi.fn(async () => "live");
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model: new MockLanguageModelV4({
          doGenerate: toolResponse([
            {
              id: "call-secret",
              input: '{"authorization":"Bearer SECRET_SENTINEL"}',
              name: "write",
            },
          ]),
        }),
        prompt: "go",
        tools: { write: tool({ execute, inputSchema: SECRET_INPUT }) },
      }),
    ).rejects.toThrow("arguments could not be recorded losslessly");

    expect(execute).not.toHaveBeenCalled();
    expect(client.lookups).toEqual([]);
  });

  it("matches static cases against arguments before recording truncates them", async () => {
    const value = "a".repeat(4_097);
    const client = new FakeClient({
      replay: replaySpec({
        cases: [
          {
            match: { value },
            match_mode: "exact",
            result: { source: "static" },
          },
        ],
        on_miss: "passthrough",
        type: "static",
      }),
    });
    const execute = vi.fn(async () => ({ source: "live" }));
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: new MockLanguageModelV4({
        doGenerate: toolResponse([
          {
            id: "call-long",
            input: JSON.stringify({ value }),
            name: "write",
          },
        ]),
      }),
      prompt: "go",
      tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
    });

    expect(result.toolResults[0]?.output).toEqual({ source: "static" });
    expect(execute).not.toHaveBeenCalled();
  });

  it("rejects prepareStep before replay can replace the model or prompt", async () => {
    const client = new FakeClient({ replay: replaySpec() });
    const model = modelForValue();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model,
        prepareStep: () => ({ model, messages: [] }),
        prompt: "go",
      }),
    ).rejects.toThrow("Replay does not support prepareStep");
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it.each([
    ["provider", { type: "provider" }],
    ["dynamic", { type: "dynamic", execute: async () => "done" }],
  ])("rejects a passthrough %s tool before model execution", async (_name, replayTool) => {
    const client = new FakeClient({ replay: replaySpec() });
    const model = modelForValue();
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model,
        prompt: "go",
        tools: { write: replayTool as never },
      }),
    ).rejects.toThrow(/provider|dynamic/);
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it("runs passthrough once with unchanged execution options", async () => {
    const client = new FakeClient({ replay: replaySpec() });
    const execute = vi.fn(async (_input, options) => options.toolCallId);
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: modelForValue(),
      prompt: "go",
      tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
    });

    expect(execute).toHaveBeenCalledOnce();
    expect(execute.mock.calls[0]?.[1].toolCallId).toBe("call-1");
    expect(result.toolResults[0]?.output).toBe("call-1");
  });

  it.each([
    {
      name: "static",
      client: () =>
        new FakeClient({
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
        }),
      expected: { source: "static" },
    },
    {
      name: "history",
      client: () =>
        new FakeClient({
          lookup: () => ({
            match: {
              error: null,
              result: { source: "history" },
              status: "completed",
            },
          }),
          replay: replaySpec({ on_miss: "fail", type: "history" }),
        }),
      expected: { source: "history" },
    },
  ])("returns a validated $name result without executing", async (scenario) => {
    const client = scenario.client();
    const execute = vi.fn(async () => ({ source: "live" }));
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: modelForValue(),
      prompt: "go",
      tools: {
        write: tool({ execute, inputSchema: VALUE_INPUT }),
      },
    });

    expect(execute).not.toHaveBeenCalled();
    expect(result.toolResults[0]?.output).toEqual(scenario.expected);
  });

  it("blocks later tools and the next model call on a history miss", async () => {
    const client = new FakeClient({
      replay: replaySpec({ on_miss: "fail", type: "history" }),
    });
    const later = vi.fn(async () => "later");
    const model = new MockLanguageModelV4({
      doGenerate: [
        toolResponse([
          { id: "call-1", input: '{"value":"a"}', name: "write" },
          { id: "call-2", input: '{"value":"b"}', name: "later" },
        ]),
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
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model,
        prompt: "go",
        stopWhen: () => false,
        tools: {
          later: tool({ execute: later, inputSchema: VALUE_INPUT }),
          write: tool({
            execute: async () => "write",
            inputSchema: VALUE_INPUT,
          }),
        },
      }),
    ).rejects.toThrow("No history result for tool 'write'");

    expect(later).not.toHaveBeenCalled();
    expect(model.doGenerateCalls).toHaveLength(1);
    expect(client.updated.at(-1)?.status).toBe("failed");
  });

  it("returns and records a completed null history result", async () => {
    const client = new FakeClient({
      lookup: () => ({
        match: { error: null, result: null, status: "completed" },
      }),
      replay: replaySpec({ on_miss: "fail", type: "history" }),
    });
    const execute = vi.fn(async () => ({ saved: true }));
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: modelForValue(),
      prompt: "go",
      tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
    });

    expect(result.toolResults[0]?.output).toBeNull();
    expect(execute).not.toHaveBeenCalled();
    expect(client.updated.at(-1)?.status).toBe("completed");
    const toolNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "tool_call");
    expect(toolNode).toMatchObject({
      attributes: { mocked: true, policy: "history" },
      outputs: null,
      status: "completed",
    });
  });

  it("throws and records a failed history result", async () => {
    const client = new FakeClient({
      lookup: () => ({
        match: {
          error: "recorded tool failure",
          result: null,
          status: "failed",
        },
      }),
      replay: replaySpec({ on_miss: "passthrough", type: "history" }),
    });
    const execute = vi.fn(async () => "live");
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model: modelForValue(),
        prompt: "go",
        tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
      }),
    ).rejects.toBeInstanceOf(ToolPolicyError);

    expect(execute).not.toHaveBeenCalled();
    expect(client.updated.at(-1)?.status).toBe("failed");
    const toolNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "tool_call");
    expect(toolNode).toMatchObject({
      attributes: { mocked: true, policy: "history" },
      error: "recorded tool failure",
      outputs: null,
      status: "failed",
    });
  });

  it("returns an error_result without validating it as a successful output", async () => {
    const client = new FakeClient({
      replay: replaySpec({
        cases: [],
        on_miss: "error_result",
        type: "static",
      }),
    });
    const execute = vi.fn(async () => ({ saved: true }));
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: modelForValue(),
      prompt: "go",
      tools: {
        write: tool({
          execute,
          inputSchema: VALUE_INPUT,
          outputSchema: SUCCESS_OUTPUT,
        }),
      },
    });

    expect(execute).not.toHaveBeenCalled();
    expect(result.toolResults[0]?.output).toEqual({
      error: "No static result for tool 'write'",
    });
    expect(client.updated.at(-1)?.status).toBe("completed");
    const toolNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "tool_call");
    expect(toolNode).toMatchObject({
      attributes: { mocked: true, policy: "static" },
      error: "No static result for tool 'write'",
      status: "failed",
    });
  });

  it("still validates successful mocked output against the tool schema", async () => {
    const client = new FakeClient({
      replay: replaySpec({
        cases: [
          {
            match: null,
            match_mode: "exact",
            result: { invalid: true },
          },
        ],
        on_miss: "fail",
        type: "static",
      }),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model: modelForValue(),
        prompt: "go",
        tools: {
          write: tool({
            execute: async () => ({ saved: true }),
            inputSchema: VALUE_INPUT,
            outputSchema: SUCCESS_OUTPUT,
          }),
        },
      }),
    ).rejects.toThrow("failed its output schema");
  });

  it("rejects successful mocked output when the output schema has no validator", async () => {
    const client = new FakeClient({
      replay: replaySpec({
        cases: [
          {
            match: null,
            match_mode: "exact",
            result: { saved: true },
          },
        ],
        on_miss: "fail",
        type: "static",
      }),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        model: modelForValue(),
        prompt: "go",
        tools: {
          write: tool({
            execute: async () => ({ saved: true }),
            inputSchema: VALUE_INPUT,
            outputSchema: jsonSchema<{ saved: boolean }>({
              additionalProperties: false,
              properties: { saved: { type: "boolean" } },
              required: ["saved"],
              type: "object",
            }),
          }),
        },
      }),
    ).rejects.toThrow("output schema has no runtime validator");
  });

  it("isolates ticket and failure state across concurrent invocations", async () => {
    const client = new FakeClient({ replay: replaySpec() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });
    const execute = vi.fn(async (input: { value: string }) => input.value);

    const results = await Promise.all([
      generate({
        model: modelForValue(),
        prompt: "one",
        tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
      }),
      generate({
        model: modelForValue(),
        prompt: "two",
        tools: { write: tool({ execute, inputSchema: VALUE_INPUT }) },
      }),
    ]);

    expect(results.map((result) => result.toolResults[0]?.output)).toEqual([
      "a",
      "a",
    ]);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["baseline", 0],
    ["agent", 1],
  ] as const)("handles repeated %s history calls", async (scope, expectedWarnings) => {
    const client = new FakeClient({
      lookup: () => ({
        match: { error: null, result: "recorded", status: "completed" },
      }),
      replay: replaySpec({ on_miss: "fail", scope, type: "history" }),
    });
    const model = new MockLanguageModelV4({
      doGenerate: toolResponse([
        { id: "call-1", input: '{"value":"a"}', name: "write" },
        { id: "call-2", input: '{"value":"a"}', name: "write" },
      ]),
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    try {
      await generate({
        model,
        prompt: "go",
        tools: {
          write: tool({
            execute: async () => "live",
            inputSchema: VALUE_INPUT,
          }),
        },
      });

      expect(warn).toHaveBeenCalledTimes(expectedWarnings);
      if (expectedWarnings > 0) {
        expect(warn.mock.calls[0]?.[0]).toContain("newest completed result");
      }
    } finally {
      warn.mockRestore();
    }
  });

  it("keeps an ordinary replay passthrough error native", async () => {
    const client = new FakeClient({ replay: replaySpec() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    const result = await generate({
      model: modelForValue(),
      prompt: "go",
      tools: {
        write: tool({
          execute: async (): Promise<string> => {
            throw new Error("application failure");
          },
          inputSchema: VALUE_INPUT,
        }),
      },
    });

    expect(result.content.some((part) => part.type === "tool-error")).toBe(
      true,
    );
    expect(client.updated.at(-1)?.status).toBe("completed");
  });
});
