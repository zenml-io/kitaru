import {
  generateText,
  isStepCount,
  jsonSchema,
  type ToolExecutionOptions,
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
