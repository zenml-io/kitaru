import { Agent } from "@mastra/core/agent";
import type { LLMStepResult } from "@mastra/core/stream";
// @ts-expect-error Mastra 1.51.0 exports this public test helper without declarations.
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";

type CompatibilityStep = LLMStepResult<unknown> & {
  model?: {
    modelId?: string;
    provider?: string;
    version?: string;
  };
};

type ModelCall = {
  maxOutputTokens?: number;
  prompt: Array<{ content: unknown; role: string }>;
  temperature?: number;
  topP?: number;
};

type ModelResult = {
  content: unknown[];
  finishReason: string;
  providerMetadata?: Record<string, unknown>;
  request?: { body?: unknown };
  response?: { id?: string; modelId?: string; timestamp?: Date };
  usage: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
  };
  warnings: unknown[];
};

const RAW_INPUT = { count: "2" };
const EXECUTION_INPUT = { count: 2, label: "default" };

function textResult(text = "done", suffix = "done"): ModelResult {
  return {
    content: [{ text, type: "text" }],
    finishReason: "stop",
    providerMetadata: { compatibility: { step: suffix } },
    request: { body: { request: suffix } },
    response: {
      id: `response-${suffix}`,
      modelId: `effective-${suffix}`,
      timestamp: new Date(0),
    },
    usage: { inputTokens: 5, outputTokens: 2, totalTokens: 7 },
    warnings: [],
  };
}

function toolResult(
  toolName = "normalize",
  toolCallId = "call-1",
  input = '{"count":"2"}',
): ModelResult {
  return {
    content: [
      {
        input,
        toolCallId,
        toolName,
        type: "tool-call",
      },
    ],
    finishReason: "tool-calls",
    providerMetadata: { compatibility: { step: "tool" } },
    request: { body: { request: "tool" } },
    response: {
      id: "response-tool",
      modelId: "effective-tool",
      timestamp: new Date(0),
    },
    usage: { inputTokens: 3, outputTokens: 4, totalTokens: 7 },
    warnings: [],
  };
}

function makeModel(
  results: Array<ModelResult | Error>,
  {
    modelId = "compatibility-model",
    provider = "compatibility-provider",
  }: { modelId?: string; provider?: string } = {},
) {
  const calls: ModelCall[] = [];
  let resultIndex = 0;
  const model = new MastraLanguageModelV2Mock({
    modelId,
    provider,
    doGenerate: async (options: ModelCall) => {
      calls.push(options);
      const result = results[resultIndex++];
      if (result instanceof Error) {
        throw result;
      }
      if (!result) {
        throw new Error("Compatibility model ran out of results");
      }
      return result;
    },
  });
  return { calls, model };
}

function normalizeTool(execute: (input: typeof EXECUTION_INPUT) => unknown) {
  return createTool({
    id: "normalize",
    description: "Normalize input using schema coercion and defaults",
    inputSchema: z.object({
      count: z.coerce.number(),
      label: z.string().default("default"),
    }),
    execute: async (input) => execute(input),
  });
}

function historyKey(toolName: string, input: unknown): string {
  return JSON.stringify({ input, toolName });
}

function toolCallId(context: unknown): string {
  if (
    typeof context === "object" &&
    context !== null &&
    "toolCallId" in context &&
    typeof context.toolCallId === "string"
  ) {
    return context.toolCallId;
  }
  throw new Error("Mastra tool hook context omitted toolCallId");
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Mastra 1.51.0 compatibility", () => {
  it("keeps raw registry keys in listTools and formats execution keys", async () => {
    const { model } = makeModel([textResult("unused")]);
    const agent = new Agent({
      id: "tool-name-agent",
      name: "Tool name agent",
      instructions: "Use the tool.",
      model,
      tools: {
        "send.email": createTool({
          id: "send.email",
          description: "Send an email",
          inputSchema: z.object({}),
          execute: async () => ({ sent: true }),
        }),
      },
    });

    expect(Object.keys(await agent.listTools())).toEqual(["send.email"]);
    expect(Object.keys(await agent.getToolsForExecution({}))).toEqual([
      "send_email",
    ]);
  });

  it("reports the stable Mastra collision error identifier", async () => {
    const { model } = makeModel([textResult("unused")]);
    const tool = (id: string) =>
      createTool({
        id,
        description: "Collision probe",
        inputSchema: z.object({}),
        execute: async () => ({ ok: true }),
      });
    const agent = new Agent({
      id: "tool-collision-agent",
      name: "Tool collision agent",
      instructions: "Use the tool.",
      model,
      tools: {
        "send.email": tool("send.email"),
        send_email: tool("send_email"),
      },
    });

    await expect(agent.getToolsForExecution({})).rejects.toMatchObject({
      id: "AGENT_TOOL_NAME_COLLISION",
    });
  });

  it("accepts public run overrides and exposes complete step-local data", async () => {
    const configured = makeModel([textResult("configured")], {
      modelId: "configured-model",
      provider: "configured-provider",
    });
    const perRun = makeModel([toolResult(), textResult()], {
      modelId: "run-model",
      provider: "run-provider",
    });
    const steps: CompatibilityStep[] = [];
    let beforeInput: unknown;
    let afterInput: unknown;
    let beforeCallId: string | undefined;
    let afterCallId: string | undefined;
    let executedInput: unknown;
    const normalize = normalizeTool((input) => {
      executedInput = input;
      return { normalized: input };
    });
    const agent = new Agent({
      id: "compatibility-agent",
      name: "Compatibility agent",
      instructions: "Configured instructions",
      model: configured.model,
      tools: { normalize },
    });

    const result = await agent.generate("Normalize two.", {
      hooks: {
        beforeToolCall: ({ context, input }) => {
          beforeInput = input;
          beforeCallId = toolCallId(context);
        },
        afterToolCall: ({ context, input }) => {
          afterInput = input;
          afterCallId = toolCallId(context);
        },
      },
      instructions: "Per-run instructions",
      maxSteps: 2,
      model: perRun.model,
      modelSettings: { temperature: 0.7 },
      onStepFinish: (step) => {
        steps.push(step);
      },
      system: "Per-run system",
    });

    expect(result.text).toBe("done");
    expect(configured.calls).toHaveLength(0);
    expect(perRun.calls).toHaveLength(2);
    expect(perRun.calls[0]?.temperature).toBe(0.7);
    expect(steps).toHaveLength(2);

    const firstStep = steps[0];
    expect(firstStep).toBeDefined();
    expect(firstStep?.toolCalls).toHaveLength(1);
    expect(firstStep?.toolResults).toHaveLength(1);
    expect(firstStep?.response).toMatchObject({
      id: "response-tool",
      modelId: "effective-tool",
    });
    expect(firstStep?.model).toEqual({
      modelId: "run-model",
      provider: "run-provider",
      version: "v2",
    });
    expect(firstStep?.usage).toMatchObject({
      inputTokens: 3,
      outputTokens: 4,
      totalTokens: 7,
    });
    expect(firstStep?.request.body).toEqual({ request: "tool" });
    expect(firstStep?.content.map((part) => part.type)).toEqual([
      "tool-call",
      "tool-result",
    ]);
    expect(firstStep?.providerMetadata).toEqual({
      compatibility: { step: "tool" },
    });

    const stepCall = firstStep?.toolCalls[0]?.payload;
    const stepResult = firstStep?.toolResults[0]?.payload;
    expect(beforeCallId).toBe("call-1");
    expect(afterCallId).toBe("call-1");
    expect(stepCall?.toolCallId).toBe("call-1");
    expect(stepResult?.toolCallId).toBe("call-1");
    expect(beforeInput).toEqual(RAW_INPUT);
    expect(afterInput).toEqual(RAW_INPUT);
    expect(stepCall?.args).toEqual(RAW_INPUT);
    expect(stepResult?.args).toEqual(RAW_INPUT);
    expect(executedInput).toEqual(EXECUTION_INPUT);
  });

  it("looks up a recorded raw input from the replay hook input", async () => {
    const history = new Map<string, unknown>();
    let recordExecutions = 0;
    const recordingModel = makeModel([toolResult(), textResult()]);
    const recordingAgent = new Agent({
      id: "recording-agent",
      name: "Recording agent",
      instructions: "Call normalize.",
      model: recordingModel.model,
      tools: {
        normalize: normalizeTool(() => {
          recordExecutions += 1;
          return { source: "recorded" };
        }),
      },
    });
    let recordedInput: unknown;

    await recordingAgent.generate("Record.", {
      maxSteps: 2,
      onStepFinish: (step) => {
        const call = step.toolCalls[0]?.payload;
        const result = step.toolResults[0]?.payload;
        if (call && result) {
          recordedInput = call.args;
          history.set(historyKey(call.toolName, call.args), result.result);
        }
      },
    });

    let replayExecutions = 0;
    let replayHookInput: unknown;
    let replayAfterCalls = 0;
    const replaySteps: CompatibilityStep[] = [];
    const replayModel = makeModel([toolResult(), textResult()]);
    const replayAgent = new Agent({
      id: "replay-agent",
      name: "Replay agent",
      instructions: "Call normalize.",
      model: replayModel.model,
      tools: {
        normalize: normalizeTool(() => {
          replayExecutions += 1;
          return { source: "unexpected-real-call" };
        }),
      },
    });

    await replayAgent.generate("Replay.", {
      hooks: {
        beforeToolCall: ({ input, toolName }) => {
          replayHookInput = input;
          const key = historyKey(toolName, input);
          if (!history.has(key)) {
            throw new Error("History lookup missed");
          }
          return { output: history.get(key), proceed: false };
        },
        afterToolCall: () => {
          replayAfterCalls += 1;
        },
      },
      maxSteps: 2,
      onStepFinish: (step) => {
        replaySteps.push(step);
      },
    });

    const replayResult = replaySteps[0]?.toolResults[0]?.payload;
    expect(recordExecutions).toBe(1);
    expect(replayExecutions).toBe(0);
    expect(replayAfterCalls).toBe(0);
    expect(replayHookInput).toEqual(recordedInput);
    expect(replayResult?.toolCallId).toBe("call-1");
    expect(replayResult?.args).toEqual(recordedInput);
    expect(replayResult?.result).toEqual({ source: "recorded" });
  });

  it("uses per-run hooks instead of configured hooks", async () => {
    const events: string[] = [];
    const model = makeModel([toolResult(), textResult()]);
    const agent = new Agent({
      id: "hook-agent",
      name: "Hook agent",
      hooks: {
        afterToolCall: () => {
          events.push("configured-after");
        },
        beforeToolCall: () => {
          events.push("configured-before");
        },
      },
      instructions: "Call normalize.",
      model: model.model,
      tools: {
        normalize: normalizeTool(() => ({ ok: true })),
      },
    });

    await agent.generate("Run.", {
      hooks: {
        afterToolCall: () => {
          events.push("run-after");
        },
        beforeToolCall: () => {
          events.push("run-before");
        },
      },
      maxSteps: 2,
    });

    expect(events).toEqual(["run-before", "run-after"]);
  });

  it("replaces instructions, appends system, and merges model settings", async () => {
    const configured = makeModel([textResult("configured")], {
      modelId: "configured-model",
    });
    const withSystem = makeModel([textResult("with-system")], {
      modelId: "with-system-model",
    });
    const withoutSystem = makeModel([textResult("without-system")], {
      modelId: "without-system-model",
    });
    const agent = new Agent({
      defaultOptions: {
        modelSettings: {
          maxOutputTokens: 17,
          temperature: 0.1,
          topP: 0.2,
        },
      },
      id: "override-agent",
      name: "Override agent",
      instructions: "Configured instructions",
      model: configured.model,
    });

    await agent.generate("Run.", {
      instructions: "Replacement instructions",
      model: withSystem.model,
      modelSettings: { temperature: 0.9 },
      system: "Caller system",
    });
    await agent.generate("Run again.", {
      instructions: "Replacement instructions",
      model: withoutSystem.model,
    });

    const firstCall = withSystem.calls[0];
    const firstSystemMessages = firstCall?.prompt
      .filter((message) => message.role === "system")
      .map((message) => message.content);
    expect(firstSystemMessages).toEqual([
      "Replacement instructions",
      "Caller system",
    ]);
    expect(firstCall).toMatchObject({
      maxOutputTokens: 17,
      temperature: 0.9,
      topP: 0.2,
    });

    const secondSystemMessages = withoutSystem.calls[0]?.prompt
      .filter((message) => message.role === "system")
      .map((message) => message.content);
    expect(secondSystemMessages).toEqual(["Replacement instructions"]);
    expect(configured.calls).toHaveLength(0);
  });

  it("reports model failure through one error step before rejecting", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const model = makeModel([new Error("model failed")]);
    const steps: CompatibilityStep[] = [];
    const agent = new Agent({
      id: "model-failure-agent",
      name: "Model failure agent",
      instructions: "Respond.",
      model: model.model,
    });

    await expect(
      agent.generate("Fail.", {
        onStepFinish: (step) => {
          steps.push(step);
        },
      }),
    ).rejects.toThrow("model failed");

    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({
      finishReason: "error",
      toolCalls: [],
      toolResults: [],
    });
  });

  it("reports tool failure to the hook and completes the failed-call step", async () => {
    const model = makeModel([
      toolResult("fail", "call-fail", '{"value":"x"}'),
      textResult("recovered"),
    ]);
    const steps: CompatibilityStep[] = [];
    const afterErrors: unknown[] = [];
    const fail = createTool({
      id: "fail",
      description: "Fail",
      inputSchema: z.object({ value: z.string() }),
      execute: async () => {
        throw new Error("tool failed");
      },
    });
    const agent = new Agent({
      id: "tool-failure-agent",
      name: "Tool failure agent",
      instructions: "Call fail.",
      model: model.model,
      tools: { fail },
    });

    const result = await agent.generate("Fail.", {
      hooks: {
        afterToolCall: ({ error }) => {
          afterErrors.push(error);
        },
      },
      maxSteps: 2,
      onStepFinish: (step) => {
        steps.push(step);
      },
    });

    expect(result.text).toBe("recovered");
    expect(afterErrors).toHaveLength(1);
    expect(afterErrors[0]).toEqual(
      expect.objectContaining({ message: "tool failed" }),
    );
    expect(steps).toHaveLength(2);
    expect(steps[0]?.toolCalls[0]?.payload.toolCallId).toBe("call-fail");
    expect(steps[0]?.toolResults).toEqual([]);
  });

  it("rejects the run when async onStepFinish rejects", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const model = makeModel([textResult()]);
    let callbackCount = 0;
    const agent = new Agent({
      id: "callback-failure-agent",
      name: "Callback failure agent",
      instructions: "Respond.",
      model: model.model,
    });

    await expect(
      agent.generate("Fail callback.", {
        onStepFinish: async () => {
          callbackCount += 1;
          throw new Error("step callback failed");
        },
      }),
    ).rejects.toThrow("step callback failed");
    expect(callbackCount).toBe(1);
  });
});
