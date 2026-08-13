// @ts-expect-error Mastra exports this public test helper without declarations.
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";

const BASELINE_INPUT =
  "Investigate account acct-1001 and delayed order ord-1001. The customer reports a suspected duplicate charge.";
const REPLAY_INPUT =
  "Priority escalation: investigate account acct-1001 and order ord-1001. Confirm the delayed order and suspected duplicate charge from tool evidence.";
const REPLAY_INSTRUCTIONS =
  "Follow the configured support workflow. Use the account and order lookup tools and queue one refund review for a delayed duplicate charge. Answer with a JSON object only, using exactly the keys decision, evidence, risk, and nextAction, and record the queued refund review under evidence.";

type ModelResult = {
  content: unknown[];
  finishReason: string;
  providerMetadata: Record<string, unknown>;
  request: { body: unknown };
  response: { id: string; modelId: string; timestamp: Date };
  usage: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
  };
  warnings: unknown[];
};

function toolResult(
  suffix: string,
  calls: Array<{ input: string; toolCallId: string; toolName: string }>,
): ModelResult {
  return {
    content: calls.map((call) => ({ ...call, type: "tool-call" })),
    finishReason: "tool-calls",
    providerMetadata: { fixture: { step: suffix } },
    request: { body: { fixtureStep: suffix } },
    response: {
      id: `fixture-response-${suffix}`,
      modelId: "gpt-5-nano-fixture",
      timestamp: new Date(0),
    },
    usage: { inputTokens: 20, outputTokens: 8, totalTokens: 28 },
    warnings: [],
  };
}

function textResult(): ModelResult {
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify({
          decision: "refund_review",
          evidence: ["order_delayed", "duplicate_charge"],
          risk: "customer_charged_twice",
          nextAction: "refund_review_queued",
        }),
      },
    ],
    finishReason: "stop",
    providerMetadata: { fixture: { step: "decision" } },
    request: { body: { fixtureStep: "decision" } },
    response: {
      id: "fixture-response-decision",
      modelId: "gpt-5-nano-fixture",
      timestamp: new Date(0),
    },
    usage: { inputTokens: 30, outputTokens: 20, totalTokens: 50 },
    warnings: [],
  };
}

export function createDeterministicModel(replay = false): unknown {
  const results = [
    toolResult("lookup", [
      {
        input: '{"accountId":"acct-1001"}',
        toolCallId: "call-account",
        toolName: "lookupAccount",
      },
      {
        input: '{"orderId":"ord-1001"}',
        toolCallId: "call-order",
        toolName: "lookupOrder",
      },
    ]),
    toolResult("action", [
      {
        input: '{"orderId":"ord-1001"}',
        toolCallId: "call-refund-review",
        toolName: "queueRefundReview",
      },
    ]),
    textResult(),
  ];
  let index = 0;
  return new MastraLanguageModelV2Mock({
    modelId: "gpt-5-nano-fixture",
    provider: "openai-fixture",
    doGenerate: async (options: unknown) => {
      const serializedOptions = JSON.stringify(options);
      const expectedInput = replay ? REPLAY_INPUT : BASELINE_INPUT;
      if (!serializedOptions.includes(expectedInput)) {
        throw new Error("Expected task input was not applied");
      }
      if (replay && !serializedOptions.includes(REPLAY_INSTRUCTIONS)) {
        throw new Error("Expected replay instructions were not applied");
      }
      const result = results[index++];
      if (!result) {
        throw new Error("Deterministic model ran out of results");
      }
      return result;
    },
  });
}
