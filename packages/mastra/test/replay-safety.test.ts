import { Agent } from "@mastra/core/agent";
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from "@mastra/core/request-context";
// @ts-expect-error Mastra 1.51.0 exports this public test helper without declarations.
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { ToolPolicyError, ToolPolicyMissError } from "@zenml-io/kitaru";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";

import { KitaruAgent } from "../src/index.js";
import { assertReplayToolCoverage } from "../src/replay-guards.js";
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
  it("checks tools present only in Mastra's executable inventory", async () => {
    await expect(
      assertReplayToolCoverage({
        agent: {
          getDefaultOptions: async () => ({}),
          getToolsForExecution: async () => ({
            frameworkTool: { description: "Runs outside the local process" },
          }),
          listConfiguredInputProcessors: async () => [],
          listTools: async () => ({}),
        },
        runtimeOptions: {},
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow(
      "Replay requires a local execute function for tool 'frameworkTool'",
    );
  });

  it("fails closed when the agent cannot expose its executable inventory", async () => {
    await expect(
      assertReplayToolCoverage({
        agent: { listTools: async () => ({}) },
        runtimeOptions: {},
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow("Replay requires Mastra Agent tool inventory support");
  });

  it.each([
    ["clientTools", { clientTools: { external: {} } }],
    ["toolsets", { toolsets: { remote: { external: {} } } }],
  ])("checks non-interceptable tools supplied through %s", async (_name, runtimeOptions) => {
    await expect(
      assertReplayToolCoverage({
        agent: {
          getDefaultOptions: async () => ({}),
          getToolsForExecution: async (options: Record<string, unknown>) =>
            options.clientTools ??
            Object.assign(
              {},
              ...Object.values(
                (options.toolsets as
                  | Record<string, Record<string, unknown>>
                  | undefined) ?? {},
              ),
            ),
          listConfiguredInputProcessors: async () => [],
          listTools: async () => ({}),
        },
        runtimeOptions,
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow(
      "Replay requires a local execute function for tool 'external'",
    );
  });

  it.each([
    ["prepareStep", { prepareStep: () => ({ tools: {} }) }],
    ["input processors", { inputProcessors: [{ id: "replace-tools" }] }],
  ])("rejects per-step mutation through %s", async (_name, runtimeOptions) => {
    const { model } = makeModel([textResult("unused")]);
    const agent = new Agent({
      id: "per-step-mutation-agent",
      instructions: "Answer.",
      model,
      name: "Per-step mutation agent",
    });

    await expect(
      assertReplayToolCoverage({
        agent,
        runtimeOptions,
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow(/prepareStep|input processors/);
  });

  it("rejects configured input processors", async () => {
    const { model } = makeModel([textResult("unused")]);
    const agent = new Agent({
      id: "configured-processor-agent",
      inputProcessors: [
        {
          id: "replace-tools",
          processInputStep: () => ({ tools: {} }),
        },
      ],
      instructions: "Answer.",
      model,
      name: "Configured processor agent",
    });

    await expect(
      assertReplayToolCoverage({
        agent,
        runtimeOptions: {},
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow("Replay does not support input processors");
  });

  it.each([
    ["memory", { memory: { resource: "user", thread: "thread" } }],
    ["input processors", { inputProcessors: [{ id: "replace-tools" }] }],
  ])("rejects unsafe default %s", async (_name, defaultOptions) => {
    await expect(
      assertReplayToolCoverage({
        agent: {
          getDefaultOptions: async () => defaultOptions,
          getToolsForExecution: async () => ({}),
          listConfiguredInputProcessors: async () => [],
          listTools: async () => ({}),
        },
        runtimeOptions: {},
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow(/default Mastra option 'memory'|input processors/);
  });

  it("checks tool names supplied through default options", async () => {
    await expect(
      assertReplayToolCoverage({
        agent: {
          getDefaultOptions: async () => ({
            clientTools: { "send.email": { execute: () => undefined } },
          }),
          getToolsForExecution: async () => ({
            send_email: { execute: () => undefined },
          }),
          listConfiguredInputProcessors: async () => [],
          listTools: async () => ({}),
        },
        runtimeOptions: {},
        spec: failingStaticSpec() as never,
      }),
    ).rejects.toThrow(
      "Replay cannot safely apply a policy for tool 'send.email'",
    );
  });

  it("rejects tool names that Mastra rewrites before the model sees them", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({
      replaySpec: {
        ...failingStaticSpec(),
        tool_policy: {
          default: { type: "passthrough" },
          tools: {
            "send.email": { cases: [], on_miss: "fail", type: "static" },
          },
        },
      },
    });
    const { calls, model } = makeModel([
      toolCallResult("send_email", "call-1"),
    ]);
    const executions = vi.fn(async () => ({ sent: true }));
    const agent = new Agent({
      id: "rewritten-tool-agent",
      instructions: "Send an email.",
      model,
      name: "Rewritten tool agent",
      tools: {
        "send.email": createTool({
          description: "Send an email",
          execute: executions,
          id: "send.email",
          inputSchema: z.object({ count: z.coerce.number() }),
        }),
      },
    });

    await expect(wrap(agent).generate("run")).rejects.toThrow(
      new ToolPolicyError(
        "Replay cannot safely apply a policy for tool 'send.email' because Mastra exposes it to the model as 'send_email'",
      ),
    );

    expect(calls).toHaveLength(0);
    expect(executions).not.toHaveBeenCalled();
    expect(api.sessionIds).toHaveLength(0);
  });

  it("rejects tool names that Mastra truncates", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({ replaySpec: failingStaticSpec() });
    const { calls, model } = makeModel([textResult("unused")]);
    const longToolName = `tool_${"x".repeat(64)}`;
    const agent = new Agent({
      id: "long-tool-agent",
      instructions: "Use the tool.",
      model,
      name: "Long tool agent",
      tools: {
        [longToolName]: createTool({
          description: "A tool with a long registry key",
          execute: async () => ({ ok: true }),
          id: longToolName,
          inputSchema: z.object({}),
        }),
      },
    });

    await expect(wrap(agent).generate("run")).rejects.toThrow(
      `Mastra exposes it to the model as '${longToolName.slice(0, 63)}'`,
    );

    expect(calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("uses request context when checking dynamic tools", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi({ replaySpec: failingStaticSpec() });
    const { calls, model } = makeModel([textResult("unused")]);
    const requestContext = new RequestContext([
      ["providerTools", true],
    ] as const);
    const agent = new Agent({
      id: "dynamic-tool-agent",
      instructions: "Search when available.",
      model,
      name: "Dynamic tool agent",
      tools: ({ requestContext: currentContext }) =>
        currentContext.get("providerTools")
          ? ({
              webSearch: {
                description: "Provider-executed web search",
                id: "webSearch",
                inputSchema: z.object({ query: z.string() }),
              },
            } as never)
          : {},
    });

    await expect(
      wrap(agent).generate("run", { requestContext }),
    ).rejects.toThrow(
      "Replay requires a local execute function for tool 'webSearch'",
    );

    expect(calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

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
    const requestContext = new RequestContext([
      [MASTRA_RESOURCE_ID_KEY, "user-from-context"],
      [MASTRA_THREAD_ID_KEY, "thread-from-context"],
      ["tenant", "tenant-1"],
    ]);
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
      requestContext,
      resourceId: "user-123",
      savePerStep: true,
      threadId: "thread-1",
    } as never);

    const effectiveRequestContext = (
      seen[0] as { requestContext?: RequestContext }
    ).requestContext;
    expect(
      effectiveRequestContext?.get(MASTRA_RESOURCE_ID_KEY),
    ).toBeUndefined();
    expect(effectiveRequestContext?.get(MASTRA_THREAD_ID_KEY)).toBeUndefined();
    expect(effectiveRequestContext?.get("tenant")).toBe("tenant-1");
    expect(requestContext.get(MASTRA_RESOURCE_ID_KEY)).toBe(
      "user-from-context",
    );
    expect(requestContext.get(MASTRA_THREAD_ID_KEY)).toBe(
      "thread-from-context",
    );

    expect(seen[0]).not.toHaveProperty("memory");
    expect(seen[0]).not.toHaveProperty("threadId");
    expect(seen[0]).not.toHaveProperty("resourceId");
    expect(seen[0]).not.toHaveProperty("savePerStep");
  });
});
