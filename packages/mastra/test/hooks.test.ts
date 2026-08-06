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

function spec(policy: Record<string, unknown>) {
  return {
    baseline_session_id: ORIGINAL_SESSION_ID,
    id: REPLAY_ID,
    override: null,
    status: "pending",
    tool_policy: { default: policy, tools: {} },
  };
}

describe("callback composition", () => {
  it("records first and composes passthrough hooks in documented order", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const events: string[] = [];
    const api = installTestApi({ replaySpec: spec({ type: "passthrough" }) });
    const agent = new FakeAgent(async (_messages, options) => {
      const result = await invokeTool(options.hooks ?? {}, {
        args: { value: "raw" },
        callId: "call-1",
        execute: () => {
          events.push("execute");
          return "real output";
        },
        output: "unused",
        toolName: "lookup",
      });
      await options.onStepFinish?.(
        toolStep("call-1", "lookup", { value: "raw" }, result),
      );
      return result;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredAfterToolCall: () => {
        events.push("configured-after");
      },
      configuredBeforeToolCall: () => {
        events.push("configured-before");
      },
      configuredOnStepFinish: () => {
        const recordedStep = api
          .nodeBatches()
          .some((batch) => batch[0]?.node_type === "llm_call");
        events.push(recordedStep ? "configured-step" : "step-before-recording");
      },
      requestedModelId: "requested-model",
    });

    await recorded.generate("run", {
      hooks: {
        afterToolCall: () => {
          events.push("caller-after");
        },
        beforeToolCall: () => {
          events.push("caller-before");
        },
      },
      onStepFinish: () => {
        events.push("caller-step");
      },
    });

    expect(events).toEqual([
      "configured-before",
      "caller-before",
      "execute",
      "configured-after",
      "caller-after",
      "configured-step",
      "caller-step",
    ]);
  });

  it("does not invoke user hooks for Kitaru-mocked calls", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: spec({
        cases: [{ match: null, match_mode: "exact", result: "mocked" }],
        on_miss: "fail",
        type: "static",
      }),
    });
    const configuredBefore = vi.fn();
    const configuredAfter = vi.fn();
    const callerBefore = vi.fn();
    const callerAfter = vi.fn();
    const agent = new FakeAgent(async (_messages, options) => {
      const result = await invokeTool(options.hooks ?? {}, {
        args: { value: "raw" },
        callId: "call-1",
        output: "real",
        toolName: "lookup",
      });
      await options.onStepFinish?.(
        toolStep("call-1", "lookup", { value: "raw" }, result),
      );
      return result;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredAfterToolCall: configuredAfter,
      configuredBeforeToolCall: configuredBefore,
      requestedModelId: "requested-model",
    });

    const result = await recorded.generate("run", {
      hooks: {
        afterToolCall: callerAfter,
        beforeToolCall: callerBefore,
      },
    });

    expect(result).toBe("mocked");
    expect(configuredBefore).not.toHaveBeenCalled();
    expect(configuredAfter).not.toHaveBeenCalled();
    expect(callerBefore).not.toHaveBeenCalled();
    expect(callerAfter).not.toHaveBeenCalled();
  });

  it("preserves a user hook failure and flushes its tool outcome", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({ replaySpec: spec({ type: "passthrough" }) });
    const original = new Error("after hook failed");
    const agent = new FakeAgent(async (_messages, options) => {
      await invokeTool(options.hooks ?? {}, {
        args: { value: "raw" },
        callId: "call-hook-failed",
        output: "real output",
        toolName: "lookup",
      });
      return "unreachable";
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredAfterToolCall: () => {
        throw original;
      },
      requestedModelId: "requested-model",
    });

    const error = await recorded.generate("run").catch((caught) => caught);

    expect(error).toBe(original);
    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.external_id === "call-hook-failed");
    expect(tool).toMatchObject({
      error: "after hook failed",
      outputs: "real output",
      status: "failed",
    });
  });

  it("honors the first user hook that skips a passthrough call", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({ replaySpec: spec({ type: "passthrough" }) });
    const callerBefore = vi.fn();
    const execute = vi.fn(() => "real");
    const agent = new FakeAgent(async (_messages, options) => {
      const result = await invokeTool(options.hooks ?? {}, {
        args: {},
        callId: "call-1",
        execute,
        output: "unused",
        toolName: "lookup",
      });
      await options.onStepFinish?.(toolStep("call-1", "lookup", {}, result));
      return result;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredBeforeToolCall: () => ({
        output: "configured output",
        proceed: false,
      }),
      requestedModelId: "requested-model",
    });

    const result = await recorded.generate("run", {
      hooks: { beforeToolCall: callerBefore },
    });

    expect(result).toBe("configured output");
    expect(callerBefore).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });
});
