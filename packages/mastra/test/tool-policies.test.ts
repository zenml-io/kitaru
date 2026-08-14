import {
  computeToolCacheKey,
  ToolPolicyError,
  ToolPolicyMissError,
} from "@zenml-io/kitaru";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KitaruAgent } from "../src/index.js";
import type { RecordedStep } from "../src/step-recorder.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  invokeTool,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
  textStep,
  toolStep,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function replaySpec(toolPolicy: Record<string, unknown>) {
  return {
    baseline_session_id: ORIGINAL_SESSION_ID,
    id: REPLAY_ID,
    override: null,
    status: "pending",
    tool_policy: toolPolicy,
  };
}

function replayAgent(
  execute: () => Promise<unknown> | unknown,
  args: unknown = { count: "2", extra: true },
) {
  return new FakeAgent(async (_messages, options) => {
    const result = await invokeTool(options.hooks ?? {}, {
      args,
      callId: "call-1",
      execute,
      output: "unused",
      toolName: "normalize",
    });
    await options.onStepFinish?.(toolStep("call-1", "normalize", args, result));
    return result;
  });
}

function wrapper(agent: FakeAgent): KitaruAgent<FakeAgent> {
  return new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "requested-model",
  });
}

describe("replay tool policies", () => {
  it("uses the first matching static case and records model-requested arguments", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({
        default: {
          cases: [
            {
              match: { count: "2" },
              match_mode: "subset",
              result: { source: "subset" },
            },
            {
              match: null,
              match_mode: "exact",
              result: { source: "wildcard" },
            },
          ],
          on_miss: "fail",
          type: "static",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => ({ source: "real" }));
    const agent = replayAgent(execute);

    const result = await wrapper(agent).generate("run");

    expect(result).toEqual({ source: "subset" });
    expect(execute).not.toHaveBeenCalled();
    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "tool_call");
    expect(tool).toMatchObject({
      attributes: { mocked: true, policy: "static" },
      inputs: { count: "2", extra: true },
      outputs: { source: "subset" },
      status: "completed",
    });
  });

  it("matches static cases against credentials before recording redacts them", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const originalInputs = {
      authorization: "Bearer SECRET_SENTINEL",
      query: "weather",
    };
    installTestApi({
      replaySpec: replaySpec({
        default: {
          cases: [
            {
              match: originalInputs,
              match_mode: "exact",
              result: { source: "static" },
            },
          ],
          on_miss: "passthrough",
          type: "static",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => ({ source: "real" }));

    const result = await wrapper(replayAgent(execute, originalInputs)).generate(
      "run",
    );

    expect(result).toEqual({ source: "static" });
    expect(execute).not.toHaveBeenCalled();
  });

  it("fails closed for an ambiguous found null history result", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      lookup: () => ({ found: true, result: null }),
      replaySpec: replaySpec({
        default: {
          on_miss: "fail",
          scope: "baseline",
          type: "history",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => "real");
    const rawInput = JSON.parse('{"__proto__":{"tenant":"a"}}') as Record<
      string,
      unknown
    >;

    await expect(
      wrapper(replayAgent(execute, rawInput)).generate("run"),
    ).rejects.toBeInstanceOf(ToolPolicyError);

    expect(execute).not.toHaveBeenCalled();
    const lookup = api.calls.find((call) => call.path.endsWith("/tool-lookup"));
    expect(lookup?.body).toEqual({
      cache_key: computeToolCacheKey("normalize", rawInput as never),
      tool_name: "normalize",
    });
  });

  it("does not look up history with redacted credentials", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({
        default: {
          on_miss: "fail",
          scope: "baseline",
          type: "history",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => "real");

    await expect(
      wrapper(
        replayAgent(execute, {
          authorization: "Bearer SECRET_SENTINEL",
        }),
      ).generate("run"),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);

    expect(execute).not.toHaveBeenCalled();
    expect(api.calls.some((call) => call.path.endsWith("/tool-lookup"))).toBe(
      false,
    );
  });

  it("returns and records a failed error_result on a static miss", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({
        default: {
          cases: [
            { match: { count: "3" }, match_mode: "exact", result: "unused" },
          ],
          on_miss: "error_result",
          type: "static",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => "real");

    const result = await wrapper(replayAgent(execute)).generate("run");

    expect(result).toEqual({
      error: "No static result for tool 'normalize'",
    });
    expect(execute).not.toHaveBeenCalled();
    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "tool_call");
    expect(tool).toMatchObject({
      attributes: { mocked: true, policy: "static" },
      status: "failed",
    });
  });

  it("passes through on a miss when configured", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: replaySpec({
        default: {
          cases: [],
          on_miss: "passthrough",
          type: "static",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => ({ source: "real" }));

    const result = await wrapper(replayAgent(execute)).generate("run");

    expect(result).toEqual({ source: "real" });
    expect(execute).toHaveBeenCalledOnce();
  });

  it("records a passthrough tool failure from afterToolCall", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({
        default: { type: "passthrough" },
        tools: {},
      }),
    });
    const failure = new Error("tool failed");
    const agent = new FakeAgent(async (_messages, options) => {
      const context = { toolCallId: "call-failed" };
      await options.hooks?.beforeToolCall?.({
        context,
        input: { value: "raw" },
        toolName: "fail",
      });
      await options.hooks?.afterToolCall?.({
        context,
        error: failure,
        input: { value: "raw" },
        toolName: "fail",
      });
      await options.onStepFinish?.({
        ...textStep("failed-tool"),
        finishReason: "tool-calls",
        toolCalls: [
          {
            payload: {
              args: { value: "raw" },
              toolCallId: "call-failed",
              toolName: "fail",
            },
          },
        ],
        toolResults: [],
      } as unknown as RecordedStep);
      return { text: "recovered" };
    });

    const result = await wrapper(agent).generate("run");

    expect(result).toEqual({ text: "recovered" });
    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.external_id === "call-failed");
    expect(tool).toMatchObject({ error: "tool failed", status: "failed" });
  });

  it("flushes a failed policy outcome under the root when no step follows", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: replaySpec({
        default: {
          cases: [],
          on_miss: "fail",
          type: "static",
        },
        tools: {},
      }),
    });
    const execute = vi.fn(() => "real");

    await expect(
      wrapper(replayAgent(execute)).generate("run"),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);

    expect(execute).not.toHaveBeenCalled();
    const batches = api.nodeBatches();
    const rootIndex = batches[0]?.[0]?.index;
    const failedTool = batches
      .flat()
      .find((node) => node.node_type === "tool_call");
    expect(failedTool).toMatchObject({
      error: "No static result for tool 'normalize'",
      parent_index: rootIndex,
      status: "failed",
    });
  });

  it("rejects the llm policy before execution", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: replaySpec({
        default: { model: "policy-model", type: "llm" },
        tools: {},
      }),
    });
    const execute = vi.fn(() => "real");

    await expect(
      wrapper(replayAgent(execute)).generate("run"),
    ).rejects.toBeInstanceOf(ToolPolicyError);

    expect(execute).not.toHaveBeenCalled();
  });
});
