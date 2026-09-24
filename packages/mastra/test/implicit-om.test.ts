import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { Memory } from "@mastra/memory";
import { expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import { textStream } from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

it("records a first turn with implicit thread-scoped OM and string models", async () => {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing memory store");
  const source = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      observationalMemory: {
        observation: {
          model: "fixture/observer",
          messageTokens: 100,
          bufferTokens: 20,
          bufferActivation: 1,
        },
        reflection: { model: "fixture/reflector", observationTokens: 100 },
      },
    },
  });
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("first answer"),
  });
  const observe = vi.fn(async () =>
    textStream(
      "<observations>First turn.</observations><current-task>Continue.</current-task>",
    ),
  );
  const observer = new MastraLanguageModelV2Mock({
    modelId: "observer",
    provider: "fixture",
    doStream: observe,
  });
  const reflect = vi.fn(async () =>
    textStream("<observations>First turn.</observations>"),
  );
  const reflector = new MastraLanguageModelV2Mock({
    modelId: "reflector",
    provider: "fixture",
    doStream: reflect,
  });
  const api = installTestApi();
  const agent = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "first-turn",
      name: "First turn",
      instructions: "Answer",
      model: actor,
      memory,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => source.settled(),
        domain,
        configuration: source.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: (id) =>
        id === "fixture/observer"
          ? observer
          : id === "fixture/reflector"
            ? reflector
            : actor,
    },
  );
  try {
    const result = await agent.stream(
      "This is the first message. ".repeat(80),
      {
        memory: { thread: "new-thread", resource: "owner" },
      },
    );
    await result.consumeStream();
    expect(await result.text).toBe("first answer");
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
    )?.body?.inputs as Record<string, { version: number; omTape: unknown[] }>;
    expect(input.mastra_memory_replay?.version).toBe(3);
    expect(input.mastra_memory_replay?.omTape.length).toBeGreaterThan(0);
    expect(JSON.stringify(input.mastra_memory_replay)).toContain(
      '"memoryStore":"in-memory"',
    );
    const observerCalls = observe.mock.calls.length;
    const reflectorCalls = reflect.mock.calls.length;
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    const replay = await agent.stream("ignored");
    await replay.consumeStream();
    expect(await replay.text).toBe("first answer");
    expect(observe).toHaveBeenCalledTimes(observerCalls);
    expect(reflect).toHaveBeenCalledTimes(reflectorCalls);
  } finally {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
    await source.settled();
    await store.close();
  }
});
