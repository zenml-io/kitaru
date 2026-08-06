import { MockLanguageModelV4 } from "ai/test";

const BASELINE_INPUT =
  "Investigate account acct-1001 and delayed order ord-1001. The customer reports a suspected duplicate charge.";
const REPLAY_INPUT =
  "Priority escalation: investigate account acct-1001 and order ord-1001. Confirm the delayed order and suspected duplicate charge from tool evidence.";
const REPLAY_INSTRUCTIONS =
  "Follow the configured support workflow. Use the account and order lookup tools, queue one refund review for a delayed duplicate charge, and return the required structured triage decision.";
const REPLAY_MODEL_ID = "openai/gpt-5-nano";

const TEST_USAGE = {
  inputTokens: {
    cacheRead: undefined,
    cacheWrite: undefined,
    noCache: 20,
    total: 20,
  },
  outputTokens: { reasoning: undefined, text: 8, total: 8 },
};

function toolResult(
  suffix: string,
  calls: Array<{ input: string; toolCallId: string; toolName: string }>,
) {
  return {
    content: calls.map((call) => ({ ...call, type: "tool-call" as const })),
    finishReason: { raw: "tool_calls", unified: "tool-calls" as const },
    providerMetadata: { fixture: { step: suffix } },
    request: { body: { fixtureStep: suffix } },
    response: {
      id: `fixture-response-${suffix}`,
      modelId: "gpt-5-nano-fixture",
      timestamp: new Date(0),
    },
    usage: TEST_USAGE,
    warnings: [],
  };
}

function textResult() {
  return {
    content: [
      {
        text: JSON.stringify({
          decision: "refund_review",
          evidence: ["order_delayed", "duplicate_charge"],
          risk: "customer_charged_twice",
          nextAction: "refund_review_queued",
        }),
        type: "text" as const,
      },
    ],
    finishReason: { raw: "stop", unified: "stop" as const },
    providerMetadata: { fixture: { step: "decision" } },
    request: { body: { fixtureStep: "decision" } },
    response: {
      id: "fixture-response-decision",
      modelId: "gpt-5-nano-fixture",
      timestamp: new Date(0),
    },
    usage: {
      inputTokens: { ...TEST_USAGE.inputTokens, total: 30, noCache: 30 },
      outputTokens: { ...TEST_USAGE.outputTokens, total: 20, text: 20 },
    },
    warnings: [],
  };
}

export function createDeterministicModel(
  modelId = "gpt-5-nano-fixture",
): MockLanguageModelV4 {
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
  let resultIndex = 0;

  return new MockLanguageModelV4({
    doGenerate: async (options) => {
      const isReplay = modelId === REPLAY_MODEL_ID;
      const serializedPrompt = JSON.stringify(options.prompt);
      const expectedInput = isReplay ? REPLAY_INPUT : BASELINE_INPUT;
      const expectedMaxOutputTokens = isReplay ? 2000 : 900;
      if (!serializedPrompt.includes(expectedInput)) {
        throw new Error(`Expected task input was not applied for ${modelId}`);
      }
      if (isReplay && !serializedPrompt.includes(REPLAY_INSTRUCTIONS)) {
        throw new Error("Expected replay instructions were not applied");
      }
      if (options.maxOutputTokens !== expectedMaxOutputTokens) {
        throw new Error(
          `Expected maxOutputTokens=${expectedMaxOutputTokens}, received ${options.maxOutputTokens}`,
        );
      }

      const result = results[resultIndex];
      resultIndex += 1;
      if (!result) {
        throw new Error("Deterministic model received too many generate calls");
      }
      return result;
    },
    modelId,
    provider: "openai-fixture",
  });
}
