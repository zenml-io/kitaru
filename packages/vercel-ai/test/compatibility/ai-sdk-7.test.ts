import {
  generateText,
  isStepCount,
  jsonSchema,
  type ToolExecutionOptions,
  ToolLoopAgent,
  tool,
} from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it } from "vitest";

import { composeStopConditions } from "../../src/failure.js";
import { ExecutionTickets } from "../../src/tickets.js";

const TEST_USAGE = {
  inputTokens: {
    cacheRead: undefined,
    cacheWrite: undefined,
    noCache: 3,
    total: 3,
  },
  outputTokens: { reasoning: undefined, text: 2, total: 2 },
};

const EMPTY_INPUT = jsonSchema<Record<string, never>>({
  additionalProperties: false,
  properties: {},
  type: "object",
});

describe("AI SDK 7 compatibility", () => {
  it("validates call options before prepareCall or provider execution", async () => {
    const events: string[] = [];
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        events.push("provider");
        throw new Error("provider should not run");
      },
    });
    const agent = new ToolLoopAgent({
      callOptionsSchema: jsonSchema<{ tenant: string }>(
        {
          additionalProperties: false,
          properties: { tenant: { type: "string" } },
          required: ["tenant"],
          type: "object",
        },
        {
          validate: (value) =>
            typeof value === "object" &&
            value !== null &&
            typeof (value as { tenant?: unknown }).tenant === "string"
              ? {
                  success: true,
                  value: value as { tenant: string },
                }
              : {
                  error: new TypeError("tenant must be a string"),
                  success: false,
                },
        },
      ),
      model,
      prepareCall: ({ options, ...call }) => {
        events.push(`prepare:${options.tenant}`);
        return call;
      },
    });

    await expect(
      agent.generate({
        options: { tenant: 42 as unknown as string },
        prompt: "go",
      }),
    ).rejects.toThrow("tenant must be a string");
    expect(events).toEqual([]);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it("applies prepared settings and runs constructor callbacks before call callbacks", async () => {
    const events: string[] = [];
    const originalModel = new MockLanguageModelV4();
    const preparedModel = new MockLanguageModelV4({
      doGenerate: {
        content: [{ text: "prepared", type: "text" }],
        finishReason: { raw: "stop", unified: "stop" },
        usage: TEST_USAGE,
        warnings: [],
      },
    });
    const agent = new ToolLoopAgent({
      callOptionsSchema: jsonSchema<{ tenant: string }>({
        additionalProperties: false,
        properties: { tenant: { type: "string" } },
        required: ["tenant"],
        type: "object",
      }),
      maxRetries: 2,
      model: originalModel,
      onEnd: () => {
        events.push("constructor:end");
      },
      onStart: () => {
        events.push("constructor:start");
      },
      onStepEnd: () => {
        events.push("constructor:step-end");
      },
      onStepStart: () => {
        events.push("constructor:step-start");
      },
      prepareCall: ({ maxRetries, options, ...call }) => {
        events.push(`prepare:${options.tenant}:${maxRetries}`);
        return {
          ...call,
          maxRetries,
          model: preparedModel,
          prompt: `Prepared for ${options.tenant}`,
        };
      },
    });

    const result = await agent.generate({
      onEnd: () => {
        events.push("call:end");
      },
      onStart: () => {
        events.push("call:start");
      },
      onStepEnd: () => {
        events.push("call:step-end");
      },
      onStepStart: () => {
        events.push("call:step-start");
      },
      options: { tenant: "acme" },
      prompt: "original",
    });

    expect(events).toEqual([
      "prepare:acme:2",
      "constructor:start",
      "call:start",
      "constructor:step-start",
      "call:step-start",
      "constructor:step-end",
      "call:step-end",
      "constructor:end",
      "call:end",
    ]);
    expect(result.text).toBe("prepared");
    expect(Object.getPrototypeOf(result)).not.toBe(Object.prototype);
    expect(originalModel.doGenerateCalls).toHaveLength(0);
    expect(preparedModel.doGenerateCalls).toHaveLength(1);
  });

  it("preserves provider error identity when retries are disabled", async () => {
    const providerError = new Error("provider failed");
    const agent = new ToolLoopAgent({
      maxRetries: 0,
      model: new MockLanguageModelV4({
        doGenerate: async () => {
          throw providerError;
        },
      }),
    });

    await expect(agent.generate({ prompt: "go" })).rejects.toBe(providerError);
  });

  it("forwards call cancellation to the provider", async () => {
    const abortReason = new Error("cancelled by caller");
    const controller = new AbortController();
    let providerSignal: AbortSignal | undefined;
    const agent = new ToolLoopAgent({
      maxRetries: 0,
      model: new MockLanguageModelV4({
        doGenerate: async ({ abortSignal }) => {
          providerSignal = abortSignal;
          controller.abort(abortReason);
          await Promise.resolve();
          throw abortSignal?.reason;
        },
      }),
    });

    await expect(
      agent.generate({ abortSignal: controller.signal, prompt: "go" }),
    ).rejects.toBe(abortReason);
    expect(providerSignal?.aborted).toBe(true);
    expect(providerSignal?.reason).toBe(abortReason);
  });

  it("forwards call timeouts to the provider abort signal", async () => {
    let providerSignal: AbortSignal | undefined;
    const agent = new ToolLoopAgent({
      maxRetries: 0,
      model: new MockLanguageModelV4({
        doGenerate: ({ abortSignal }) =>
          new Promise((_, reject) => {
            providerSignal = abortSignal;
            abortSignal?.addEventListener(
              "abort",
              () => reject(abortSignal.reason),
              { once: true },
            );
          }),
      }),
    });

    await expect(
      agent.generate({ prompt: "go", timeout: 5 }),
    ).rejects.toBeDefined();
    expect(providerSignal?.aborted).toBe(true);
  });

  it("registers ordered parsed calls before serialized local execution", async () => {
    const events: string[] = [];
    const tickets = new ExecutionTickets();
    const model = new MockLanguageModelV4({
      doGenerate: {
        content: [
          {
            input: "{}",
            toolCallId: "call-1",
            toolName: "first",
            type: "tool-call",
          },
          {
            input: "{}",
            toolCallId: "call-2",
            toolName: "second",
            type: "tool-call",
          },
        ],
        finishReason: { raw: "tool_calls", unified: "tool-calls" },
        usage: TEST_USAGE,
        warnings: [],
      },
    });

    const wrap = (execute: () => Promise<string>) =>
      async function wrapped(
        _input: Record<string, never>,
        options: ToolExecutionOptions<Record<string, never>>,
      ) {
        return await tickets.run(options.toolCallId, execute);
      };

    await generateText({
      maxRetries: 0,
      model,
      onLanguageModelCallEnd: ({ content }) => {
        const calls = content.flatMap((part) =>
          part.type === "tool-call" &&
          !part.invalid &&
          !part.providerExecuted &&
          (part.toolName === "first" || part.toolName === "second")
            ? [part]
            : [],
        );
        events.push(
          `model-end:${calls.map((call) => call.toolCallId).join(",")}`,
        );
        tickets.register(calls.map((call) => call.toolCallId));
      },
      prompt: "Run both tools",
      tools: {
        first: tool({
          execute: wrap(async () => {
            events.push("first:start");
            await Promise.resolve();
            events.push("first:end");
            return "first";
          }),
          inputSchema: EMPTY_INPUT,
        }),
        second: tool({
          execute: wrap(async () => {
            events.push("second:start");
            events.push("second:end");
            return "second";
          }),
          inputSchema: EMPTY_INPUT,
        }),
      },
    });

    expect(events).toEqual([
      "model-end:call-1,call-2",
      "first:start",
      "first:end",
      "second:start",
      "second:end",
    ]);
  });

  it("stops before the next model call after a swallowed step callback error", async () => {
    const callbackError = new Error("step upload failed");
    let storedError: unknown;
    const failureState = {
      get failure(): unknown {
        return storedError;
      },
    };
    const model = new MockLanguageModelV4({
      doGenerate: [
        {
          content: [
            {
              input: "{}",
              toolCallId: "call-1",
              toolName: "work",
              type: "tool-call",
            },
          ],
          finishReason: { raw: "tool_calls", unified: "tool-calls" },
          usage: TEST_USAGE,
          warnings: [],
        },
        {
          content: [{ text: "second call", type: "text" }],
          finishReason: { raw: "stop", unified: "stop" },
          usage: TEST_USAGE,
          warnings: [],
        },
      ],
    });

    const run = async () => {
      const result = await generateText({
        maxRetries: 0,
        model,
        onStepEnd: async () => {
          storedError ??= callbackError;
          throw callbackError;
        },
        prompt: "Run work",
        stopWhen: composeStopConditions(failureState, isStepCount(2)),
        tools: {
          work: tool({
            execute: async () => "done",
            inputSchema: EMPTY_INPUT,
          }),
        },
      });
      if (storedError !== undefined) {
        throw storedError;
      }
      return result;
    };

    await expect(run()).rejects.toBe(callbackError);
    expect(model.doGenerateCalls).toHaveLength(1);
  });
});
