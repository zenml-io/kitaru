import { MockLanguageModelV4 } from "ai/test";

import {
  BASELINE_PROMPT,
  REPLAY_INSTRUCTIONS,
  REPLAY_PROMPT,
} from "./prompts.js";

// A provider serves its own model id, never the "provider/model" id Kitaru
// records as the requested model, so the fixture is shaped the same way.
const FIXTURE_MODEL_ID = "gpt-5-nano-fixture";

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
      modelId: FIXTURE_MODEL_ID,
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
      modelId: FIXTURE_MODEL_ID,
      timestamp: new Date(0),
    },
    usage: {
      inputTokens: { ...TEST_USAGE.inputTokens, total: 30, noCache: 30 },
      outputTokens: { ...TEST_USAGE.outputTokens, total: 20, text: 20 },
    },
    warnings: [],
  };
}

export function createDeterministicModel(replay = false): MockLanguageModelV4 {
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
      const serializedPrompt = JSON.stringify(options.prompt);
      const expectedInput = replay ? REPLAY_PROMPT : BASELINE_PROMPT;
      const expectedMaxOutputTokens = replay ? 3000 : 2000;
      if (!serializedPrompt.includes(expectedInput)) {
        throw new Error(
          `Expected ${replay ? "replay" : "baseline"} input was not applied`,
        );
      }
      if (replay && !serializedPrompt.includes(REPLAY_INSTRUCTIONS)) {
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
    modelId: FIXTURE_MODEL_ID,
    provider: "openai-fixture",
  });
}
