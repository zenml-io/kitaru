import {
  createScorer,
  type ScorerRunInputForAgent,
  type ScorerRunOutputForAgent,
} from "@mastra/core/evals";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import type { SessionView } from "@zenml-io/kitaru/evaluator";
import { describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";

import { createMastraEvaluator } from "../src/scorers.js";

const conversation = [
  { role: "user", content: "Check order 12" },
  { role: "assistant", content: "Order 12 shipped" },
  { role: "user", content: "When will it arrive?" },
  { role: "assistant", content: "Tomorrow" },
];
const view = {
  session: {
    id: "recorded-session",
    inputs: { conversation },
    outputs: "Tomorrow",
  },
  nodes: [
    {
      node_type: "tool_call",
      name: "lookup",
      inputs: { order: 12 },
      outputs: { arrival: "tomorrow" },
    },
  ],
} as unknown as SessionView;

interface ConversationInput {
  conversation: typeof conversation;
  tools: SessionView["nodes"];
}

function mapInput(session: SessionView) {
  const inputs = session.session.inputs as {
    conversation: typeof conversation;
  };
  if (typeof session.session.outputs !== "string")
    throw new Error("Expected recorded text output");
  return {
    input: { conversation: inputs.conversation, tools: session.nodes },
    output: session.session.outputs,
  };
}

describe("Mastra scorer evaluators", () => {
  it("runs actual createScorer pipelines against all turns and tool records", async () => {
    const evaluator = createMastraEvaluator({
      scorers: (params) => ({
        "conversation-coverage": createScorer<ConversationInput, string>({
          id: "native",
          description: "Inspect the complete transcript",
        })
          .generateScore(({ run }) => {
            expect(run.input?.conversation).toEqual(conversation);
            expect(run.input?.tools).toEqual(view.nodes);
            expect(run.output).toBe("Tomorrow");
            return run.input?.conversation.length === 4
              ? Number(params.score)
              : 0;
          })
          .generateReason(() => "Checked four turns and the lookup result"),
        "tool-coverage": createScorer<ConversationInput, string>({
          id: "tools",
          description: "Count tool records",
        }).generateScore(({ run }) => run.input?.tools.length ?? 0),
      }),
      mapInput,
    });
    await expect(evaluator(view, { score: 0.8 })).resolves.toEqual([
      {
        name: "conversation-coverage",
        score: 0.8,
        explanation: "Checked four turns and the lookup result",
      },
      { name: "tool-coverage", score: 1, explanation: undefined },
    ]);
  });

  it("accepts the native agent scorer input and output types", async () => {
    const nativeInput: ScorerRunInputForAgent = {
      inputMessages: [],
      rememberedMessages: [],
      systemMessages: [],
      taggedSystemMessages: {},
    };
    const nativeOutput: ScorerRunOutputForAgent = [];
    const evaluator = createMastraEvaluator({
      scorers: () => ({
        native: createScorer({
          id: "agent",
          description: "Native agent scorer",
          type: "agent",
        }).generateScore(
          ({ run }) => run.input?.rememberedMessages.length ?? -1,
        ),
      }),
      mapInput: () => ({ input: nativeInput, output: nativeOutput }),
    });
    expect(await evaluator(view, {})).toEqual([
      { name: "native", score: 0, explanation: undefined },
    ]);
  });

  it("runs a prompt-based scorer using a mocked judge model", async () => {
    const modelCall = vi.fn(async (_options: unknown) => ({
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: "text-start", id: "judge-text" });
          controller.enqueue({
            type: "text-delta",
            id: "judge-text",
            delta:
              '{"score":0.75,"reason":"All four turns and the lookup are consistent"}',
          });
          controller.enqueue({ type: "text-end", id: "judge-text" });
          controller.enqueue({
            type: "finish",
            finishReason: "stop",
            usage: { inputTokens: 20, outputTokens: 10, totalTokens: 30 },
          });
          controller.close();
        },
      }),
    }));
    const model = new MastraLanguageModelV2Mock({ doStream: modelCall });
    const evaluator = createMastraEvaluator({
      scorers: () => ({
        judged: createScorer<ConversationInput, string>({
          id: "judge",
          description: "Judge a whole conversation",
          judge: {
            model,
            instructions: "Evaluate conversation and tool evidence",
          },
        })
          .analyze({
            description: "Inspect every turn",
            outputSchema: z.object({ score: z.number(), reason: z.string() }),
            createPrompt: ({ run }) => JSON.stringify(run.input),
          })
          .generateScore(({ results }) => results.analyzeStepResult.score)
          .generateReason(({ results }) => results.analyzeStepResult.reason),
      }),
      mapInput,
    });
    await expect(evaluator(view, {})).resolves.toEqual([
      {
        name: "judged",
        score: 0.75,
        explanation: "All four turns and the lookup are consistent",
      },
    ]);
    expect(modelCall).toHaveBeenCalled();
    const sent = JSON.stringify(modelCall.mock.calls);
    expect(sent).toContain("Check order 12");
    expect(sent).toContain("When will it arrive?");
    expect(sent).toContain("lookup");
  });

  it("fails the whole evaluation when a later native scorer throws", async () => {
    const evaluator = createMastraEvaluator({
      scorers: () => ({
        first: createScorer({
          id: "first",
          description: "Succeeds",
        }).generateScore(() => 1),
        second: createScorer({
          id: "second",
          description: "Fails",
        }).generateScore(() => {
          throw new Error("judge unavailable");
        }),
      }),
      mapInput,
    });
    await expect(evaluator(view, {})).rejects.toThrow();
  });

  it.each([
    NaN,
    Infinity,
  ])("rejects a nonfinite native score (%s)", async (score) => {
    const evaluator = createMastraEvaluator({
      scorers: () => ({
        bad: createScorer({
          id: "bad",
          description: "Bad result",
        }).generateScore(() => score),
      }),
      mapInput,
    });
    await expect(evaluator(view, {})).rejects.toThrow();
  });

  it("rejects invalid names before calling a judge", async () => {
    const run = vi.fn(async () => ({ score: 1 }));
    const evaluator = createMastraEvaluator({
      scorers: () => ({ "bad name": { run } }),
      mapInput,
    });
    await expect(evaluator(view, {})).rejects.toThrow("Evaluation name");
    expect(run).not.toHaveBeenCalled();
  });

  it("propagates mapping and factory failures without scoring", async () => {
    const run = vi.fn(async () => ({ score: 1 }));
    const evaluator = createMastraEvaluator({
      scorers: () => ({ valid: { run } }),
      mapInput: () => {
        throw new Error("Missing conversation");
      },
    });
    await expect(evaluator(view, {})).rejects.toThrow("Missing conversation");
    expect(run).not.toHaveBeenCalled();
    const failedFactory = createMastraEvaluator({
      scorers: () => {
        throw new Error("Invalid judge config");
      },
      mapInput,
    });
    await expect(failedFactory(view, {})).rejects.toThrow(
      "Invalid judge config",
    );
  });
});
