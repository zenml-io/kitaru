import { Agent } from "@mastra/core/agent";
import type { InputProcessor } from "@mastra/core/processors";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  settleBuffering,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

afterEach(async () => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  await settleBuffering();
});

it("retains invocation memory tool identity through public native conversion", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const marker = "kitaru-owned-memory-identity";
  const memory = new Proxy(runtime.memory, {
    get(target, key) {
      if (key === "listTools")
        return (...args: Parameters<typeof target.listTools>) =>
          Object.fromEntries(
            Object.entries(target.listTools(...args)).map(([name, tool]) => [
              name,
              { ...tool, id: marker },
            ]),
          );
      const value = Reflect.get(target, key, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const identities: unknown[] = [];
  const executions: string[] = [];
  const initial: InputProcessor = {
    id: "identity-first",
    processInputStep({ tools }) {
      identities.push((tools?.updateWorkingMemory as { id?: string })?.id);
    },
  };
  const final: InputProcessor = {
    id: "identity-last",
    processInputStep({ tools }) {
      const wrapped = Object.fromEntries(
        Object.entries(tools ?? {}).map(([name, tool]) => {
          const native = tool as { execute?: (...args: unknown[]) => unknown };
          return [
            name,
            {
              ...native,
              execute: async (...args: unknown[]) => {
                executions.push(name);
                return native.execute?.(...args);
              },
            },
          ];
        }),
      );
      return { tools: wrapped };
    },
  };
  let calls = 0;
  const agent = new Agent({
    id: "marker-proof",
    name: "Marker proof",
    instructions: "Update memory",
    memory,
    inputProcessors: [initial, final],
    model: new MastraLanguageModelV2Mock({
      doStream: async () =>
        ++calls === 1
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolCallId: "memory-call",
                  toolName: "updateWorkingMemory",
                  input: JSON.stringify({ memory: { preference: "green" } }),
                },
              ],
              "tool-calls",
            )
          : textStream("done"),
    }),
  });
  const result = await agent.stream("Green please", {
    memory: { thread: THREAD, resource: RESOURCE },
    maxSteps: 3,
  });
  await result.consumeStream();
  await runtime.memory.settled();
  expect(identities).toEqual([marker, marker]);
  expect(executions).toEqual(["updateWorkingMemory"]);
  expect(
    await runtime.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("green");
  await runtime.store.close();
});

it("executes a second-step memory tool during replay", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const { createTool } = await import("@mastra/core/tools");
  const { z } = await import("zod/v4");
  const access = createProcessLocalMemoryAccess();
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: null,
      tool_policy: {
        default: { type: "history", scope: "baseline", on_miss: "fail" },
        tools: { advance: { type: "passthrough" } },
      },
    },
  });
  const advance = vi.fn(async () => ({ next: true }));
  let calls = 0;
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      const step = ++calls % 3;
      if (step === 1)
        return streamParts(
          [
            {
              type: "tool-call",
              toolCallId: `advance-${calls}`,
              toolName: "advance",
              input: "{}",
            },
          ],
          "tool-calls",
        );
      if (step === 2)
        return streamParts(
          [
            {
              type: "tool-call",
              toolCallId: `memory-${calls}`,
              toolName: "updateWorkingMemory",
              input: JSON.stringify({ memory: { preference: "green" } }),
            },
          ],
          "tool-calls",
        );
      return textStream("done");
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "second-step-memory",
      name: "Second-step memory",
      instructions: "Update memory after advancing",
      model,
      memory,
      defaultOptions: { maxSteps: 4 },
      tools: {
        advance: createTool({
          id: "advance",
          description: "Advance to the next step",
          inputSchema: z.object({}),
          execute: advance,
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : model,
    },
  );
  try {
    const baseline = await adapter.stream("Remember green", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await baseline.consumeStream();
    await vi.waitFor(() =>
      expect(
        api.calls.some(
          (call) =>
            call.method === "PATCH" &&
            (call.body?.metadata as Record<string, unknown> | undefined)
              ?.mastra_replay_state === "eligible",
        ),
      ).toBe(true),
    );
    const input = api.calls.find(
      (call) =>
        call.method === "PATCH" &&
        (call.body?.metadata as Record<string, unknown> | undefined)
          ?.mastra_replay_state === "eligible",
    )?.body?.inputs;
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    expect(await replay.text).toBe("done");
    expect(advance).toHaveBeenCalledTimes(2);
    expect(api.calls.some((call) => call.path.endsWith("/tool-lookup"))).toBe(
      false,
    );
  } finally {
    await runtime.store.close();
  }
});
