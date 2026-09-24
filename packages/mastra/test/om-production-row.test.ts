import { ModelRouterLanguageModel } from "@mastra/core/llm";
import type { MemoryStorage } from "@mastra/core/storage";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { Memory } from "@mastra/memory";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import { decodeMemoryReplayEnvelope } from "../src/memory-snapshot.js";
import { getNativeOMRecordConfig, textStream } from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  type ApiCall,
  installTestApi,
  REPLAY_ID,
} from "./helpers.js";

const THREAD = "row-thread";
const RESOURCE = "row-resource";
const TURNS = 5;

// Default continuation hints put Mastra's built-in extractors into the record.
const OPTIONS = {
  lastMessages: 20,
  observationalMemory: {
    observation: {
      model: "fixture/observer",
      messageTokens: 300,
      bufferTokens: 0.2,
      bufferActivation: 1,
      blockAfter: 1.1,
    },
    reflection: {
      model: "fixture/reflector",
      observationTokens: 200,
      bufferActivation: 1,
    },
  },
};

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

function omOutput(kind: "observer" | "reflector") {
  return textStream(
    kind === "observer"
      ? `<observations>\n${"The user keeps sharing travel plans for the spring trip. ".repeat(12)}\n</observations>\n<current-task>Continue.</current-task>`
      : "<observations>\nREFLECTED: the user is planning a spring trip.\n</observations>",
  );
}

function omModel(kind: "observer" | "reflector", router: boolean) {
  const doStream = vi.fn(async () => omOutput(kind));
  if (!router)
    return {
      doStream,
      model: new MastraLanguageModelV2Mock({
        modelId: kind,
        provider: "fixture",
        doStream,
      }),
    };
  // The router model carries the gateway catalog as enumerable state.
  const model = new ModelRouterLanguageModel("openai/gpt-5-nano");
  Object.assign(model, { doStream });
  return { doStream, model };
}

// Store `config` the way a JSON column does, as PostgreSQL and LibSQL do.
function storeConfigAsJson(domain: MemoryStorage): void {
  const initialize = domain.initializeObservationalMemory.bind(domain);
  const reflect = domain.createReflectionGeneration.bind(domain);
  domain.initializeObservationalMemory = (input) =>
    initialize({ ...input, config: JSON.parse(JSON.stringify(input.config)) });
  domain.createReflectionGeneration = (input) =>
    reflect({
      ...input,
      currentRecord: {
        ...input.currentRecord,
        config: JSON.parse(JSON.stringify(input.currentRecord.config)),
      },
    });
}

function lastUpdate(calls: ApiCall[], sessionId: string) {
  return calls.findLast(
    (call) => call.method === "PATCH" && call.path.endsWith(sessionId),
  )?.body;
}

it.each([
  { store: "in-memory", model: "mock" },
  { store: "in-memory", model: "router" },
  { store: "json", model: "mock" },
  { store: "json", model: "router" },
] as const)(
  "keeps Kitaru objects out of production OM records ($store store, $model model)",
  async ({ store: storeKind, model: modelKind }) => {
    vi.stubEnv("OPENAI_API_KEY", "sk-test-placeholder");
    const nativeConfig = JSON.stringify(
      await getNativeOMRecordConfig(
        OPTIONS,
        storeKind === "json" ? storeConfigAsJson : undefined,
      ),
    );
    const store = new InMemoryStore();
    const domain = store.stores.memory as MemoryStorage;
    if (storeKind === "json") storeConfigAsJson(domain);
    const source = new Memory({ storage: store, options: OPTIONS });
    const observer = omModel("observer", modelKind === "router");
    const reflector = omModel("reflector", modelKind === "router");
    const actor = new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async () => textStream("noted"),
    });
    const api = installTestApi();
    const agent = createMemoryReplayAgent(
      ({ memory }) => ({
        id: "om-row",
        name: "OM row",
        instructions: "Answer briefly.",
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
            ? observer.model
            : id === "fixture/reflector"
              ? reflector.model
              : actor,
      },
    );
    try {
      const turns: Record<string, unknown>[] = [];
      for (let turn = 1; turn <= TURNS; turn += 1) {
        const result = await agent.stream(
          `Turn ${turn}: ${"Here are more details about the spring trip. ".repeat(20)}`,
          { memory: { thread: THREAD, resource: RESOURCE } },
        );
        await result.consumeStream();
        expect(await result.text).toBe("noted");
        const sessionId = api.sessionIds[turn - 1] as string;
        await vi.waitFor(
          () =>
            expect(lastUpdate(api.calls, sessionId)?.metadata).toHaveProperty(
              "mastra_replay_state",
            ),
          { timeout: 10_000 },
        );
        const body = lastUpdate(api.calls, sessionId);
        expect(body?.metadata, `turn ${turn}`).toMatchObject({
          mastra_replay_state: "eligible",
        });
        turns.push(body?.inputs as Record<string, unknown>);

        const rows = await domain.getObservationalMemoryHistory(
          THREAD,
          RESOURCE,
        );
        expect(rows.length).toBeGreaterThan(0);
        for (const row of rows) {
          expect(row.config.observation).toMatchObject({
            model: "fixture/observer",
          });
          expect(row.config.reflection).toMatchObject({
            model: "fixture/reflector",
          });
          expect(JSON.stringify(row.config)).toBe(nativeConfig);
        }
      }

      const envelopes = turns.map((inputs) =>
        decodeMemoryReplayEnvelope(inputs.mastra_memory_replay),
      );
      const reflectionTurn = envelopes.findIndex(({ omTape }) =>
        omTape?.some(
          (entry) =>
            (entry as { phase?: unknown } | null)?.phase === "reflector",
        ),
      );
      expect(reflectionTurn).toBeGreaterThan(0);
      const observerCalls = observer.doStream.mock.calls.length;
      const reflectorCalls = reflector.doStream.mock.calls.length;
      for (const index of [reflectionTurn, TURNS - 1]) {
        vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
        vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(turns[index]));
        const replay = await agent.stream("ignored");
        await replay.consumeStream();
        expect(await replay.text).toBe("noted");
        const replaySession = api.sessionIds.at(-1) as string;
        await vi.waitFor(
          () =>
            expect(lastUpdate(api.calls, replaySession)).toMatchObject({
              status: "completed",
            }),
          { timeout: 10_000 },
        );
      }
      expect(observer.doStream).toHaveBeenCalledTimes(observerCalls);
      expect(reflector.doStream).toHaveBeenCalledTimes(reflectorCalls);
    } finally {
      await source.settled();
      await store.close();
    }
  },
  60_000,
);
