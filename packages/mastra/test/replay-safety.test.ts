import { Agent } from "@mastra/core/agent";
// @ts-expect-error Mastra 1.51.0 exports this public test helper without declarations.
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { ToolPolicyMissError } from "@zenml-io/kitaru";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

type ModelResult = {
  content: unknown[];
  finishReason: string;
  response: { id: string; modelId: string; timestamp: Date };
  usage: { inputTokens: number; outputTokens: number; totalTokens: number };
  warnings: unknown[];
};

function toolCallResult(toolName: string, toolCallId: string): ModelResult {
  return {
    content: [
      { input: '{"count":"2"}', toolCallId, toolName, type: "tool-call" },
    ],
    finishReason: "tool-calls",
    response: {
      id: `response-${toolCallId}`,
      modelId: "m",
      timestamp: new Date(0),
    },
    usage: { inputTokens: 3, outputTokens: 4, totalTokens: 7 },
    warnings: [],
  };
}

function textResult(text: string): ModelResult {
  return {
    content: [{ text, type: "text" }],
    finishReason: "stop",
    response: { id: `response-${text}`, modelId: "m", timestamp: new Date(0) },
    usage: { inputTokens: 5, outputTokens: 2, totalTokens: 7 },
    warnings: [],
  };
}

function makeModel(results: ModelResult[]) {
  const calls: unknown[] = [];
  const model = new MastraLanguageModelV2Mock({
    doGenerate: async (options: unknown) => {
      calls.push(options);
      const result = results[calls.length - 1];
      if (!result) {
        throw new Error("Replay model ran out of results");
      }
      return result;
    },
    modelId: "replay-model",
    provider: "replay-provider",
  });
  return { calls, model };
}

function failingStaticSpec() {
  return {
    baseline_session_id: ORIGINAL_SESSION_ID,
    id: REPLAY_ID,
    override: null,
    status: "pending",
    tool_policy: {
      default: { cases: [], on_miss: "fail", type: "static" },
      tools: {},
    },
  };
}

function wrap(agent: Agent) {
  return new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "requested-model",
  });
}

describe("replay safety with a real Mastra agent", () => {
  it("aborts the run instead of executing tools live after a policy miss", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({ replaySpec: failingStaticSpec() });
    const { calls, model } = makeModel([
      toolCallResult("normalize", "call-1"),
      textResult("done-live"),
    ]);
    const executions: unknown[] = [];
    const normalize = createTool({
      description: "Normalize the input",
      execute: async (input) => {
        executions.push(input);
        return { normalized: true };
      },
      id: "normalize",
      inputSchema: z.object({ count: z.coerce.number() }),
    });
    const agent = new Agent({
      id: "replay-agent",
      instructions: "Call normalize.",
      model,
      name: "Replay agent",
      tools: { normalize },
    });

    await expect(
      wrap(agent).generate("run", { maxSteps: 3 }),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);

    expect(executions).toHaveLength(0);
    expect(calls).toHaveLength(1);
    const statuses = api.calls
      .filter((call) => call.method === "PATCH")
      .map((call) => call.body?.status);
    expect(statuses).toEqual(["failed"]);
  });

  it("keeps a later side-effect tool from firing after a policy miss", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: {
        ...failingStaticSpec(),
        tool_policy: {
          default: { type: "passthrough" },
          tools: { normalize: { cases: [], on_miss: "fail", type: "static" } },
        },
      },
    });
    const { calls, model } = makeModel([
      {
        ...toolCallResult("normalize", "call-1"),
        content: [
          {
            input: '{"count":"2"}',
            toolCallId: "call-1",
            toolName: "normalize",
            type: "tool-call",
          },
          {
            input: "{}",
            toolCallId: "call-2",
            toolName: "sendEmail",
            type: "tool-call",
          },
        ],
      },
      textResult("done-live"),
    ]);
    const sends: string[] = [];
    const agent = new Agent({
      id: "side-effect-agent",
      instructions: "Call the tools.",
      model,
      name: "Side effect agent",
      tools: {
        normalize: createTool({
          description: "Normalize the input",
          execute: async () => ({ normalized: true }),
          id: "normalize",
          inputSchema: z.object({ count: z.coerce.number() }),
        }),
        sendEmail: createTool({
          description: "Send an email",
          execute: async () => {
            sends.push("sent");
            return { sent: true };
          },
          id: "sendEmail",
          inputSchema: z.object({}),
        }),
      },
    });

    await expect(
      wrap(agent).generate("run", { maxSteps: 3 }),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);

    expect(sends).toHaveLength(0);
    expect(calls).toHaveLength(1);
  });

  it("refuses a replay whose tool cannot be intercepted", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({ replaySpec: failingStaticSpec() });
    const { calls, model } = makeModel([textResult("unused")]);
    const agent = new Agent({
      id: "provider-tool-agent",
      instructions: "Search the web.",
      model,
      name: "Provider tool agent",
      tools: {
        webSearch: {
          description: "Provider-executed web search",
          id: "webSearch",
          inputSchema: z.object({ query: z.string() }),
        } as never,
      },
    });

    await expect(wrap(agent).generate("run")).rejects.toThrow(
      "Replay requires a local execute function for tool 'webSearch'",
    );

    expect(calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("keeps a replay off live memory threads", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    installTestApi({
      replaySpec: {
        ...failingStaticSpec(),
        tool_policy: { default: { type: "passthrough" }, tools: {} },
      },
    });
    const { model } = makeModel([textResult("done")]);
    const agent = new Agent({
      id: "memory-agent",
      instructions: "Answer.",
      model,
      name: "Memory agent",
    });
    const seen: unknown[] = [];
    const generate = agent.generate.bind(agent);
    vi.spyOn(agent, "generate").mockImplementation((async (
      messages: never,
      options: never,
    ) => {
      seen.push(options);
      return generate(messages, options);
    }) as never);

    await wrap(agent).generate("run", {
      memory: { resource: "user-123", thread: "thread-1" },
      resourceId: "user-123",
      savePerStep: true,
      threadId: "thread-1",
    } as never);

    expect(seen[0]).not.toHaveProperty("memory");
    expect(seen[0]).not.toHaveProperty("threadId");
    expect(seen[0]).not.toHaveProperty("resourceId");
    expect(seen[0]).not.toHaveProperty("savePerStep");
  });
});
