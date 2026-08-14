import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  invokeTool,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
  toolStep,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

describe("concurrent wrapper runs", () => {
  it("isolates sessions, sequences, callbacks, and same-ID tool ledgers", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: {
        baseline_session_id: ORIGINAL_SESSION_ID,
        id: REPLAY_ID,
        override: null,
        status: "pending",
        tool_policy: {
          default: {
            cases: [
              {
                match: { run: "a" },
                match_mode: "exact",
                result: { result: "a" },
              },
              {
                match: { run: "b" },
                match_mode: "exact",
                result: { result: "b" },
              },
            ],
            on_miss: "fail",
            type: "static",
          },
          tools: {},
        },
      },
    });
    const agent = new FakeAgent(async (_messages, options) => {
      const run = String(options.runLabel);
      await delay(run === "a" ? 15 : 1);
      const args = { run };
      const result = await invokeTool(options.hooks ?? {}, {
        args,
        callId: "shared-call-id",
        output: "real",
        toolName: "lookup",
      });
      await options.onStepFinish?.(
        toolStep("shared-call-id", "lookup", args, result),
      );
      return result;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });
    const callbackEvents: string[] = [];

    const [first, second] = await Promise.all([
      recorded.generate("caller a", {
        onStepFinish: () => {
          callbackEvents.push("a");
        },
        runLabel: "a",
      }),
      recorded.generate("caller b", {
        onStepFinish: () => {
          callbackEvents.push("b");
        },
        runLabel: "b",
      }),
    ]);

    expect(first).toEqual({ result: "a" });
    expect(second).toEqual({ result: "b" });
    expect(callbackEvents.sort()).toEqual(["a", "b"]);
    expect(api.sessionIds).toHaveLength(2);

    const roots = api.sessionIds.map((sessionId) => {
      const batches = api.nodeBatches(sessionId);
      expect(batches.map((batch) => batch[0]?.index)).toEqual([0, 1, 0]);
      return batches[0]?.[0]?.index;
    });
    expect(roots).toEqual([0, 0]);

    const tools = api.sessionIds.map((sessionId) =>
      api
        .nodeBatches(sessionId)
        .flat()
        .find((node) => node.node_type === "tool_call"),
    );
    expect(tools.map((tool) => tool?.inputs)).toEqual([
      { run: "a" },
      { run: "b" },
    ]);
    expect(tools.map((tool) => tool?.outputs)).toEqual([
      { result: "a" },
      { result: "b" },
    ]);
  });
});
