import { randomUUID } from "node:crypto";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { Memory } from "@mastra/memory";
import { PostgresStore } from "@mastra/pg";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import { decodeMemoryReplayEnvelope } from "../src/memory-snapshot.js";
import { textStream } from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  type ApiCall,
  installTestApi,
  REPLAY_ID,
} from "./helpers.js";

// A PostgreSQL connection URL, such as the repository's local test database
// on localhost:5433. The test creates and drops its own schema.
const POSTGRES_URL = process.env.KITARU_TEST_MASTRA_POSTGRES_URL;
const THREAD = "postgres-thread";
const RESOURCE = "postgres-resource";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

function omModel(kind: "observer" | "reflector") {
  const doStream = vi.fn(async () =>
    textStream(
      kind === "observer"
        ? `<observations>\n${"The user keeps sharing travel plans for the spring trip. ".repeat(12)}\n</observations>\n<current-task>Continue.</current-task>`
        : "<observations>\nREFLECTED: the user is planning a spring trip.\n</observations>",
    ),
  );
  return {
    doStream,
    model: new MastraLanguageModelV2Mock({
      modelId: kind,
      provider: "fixture",
      doStream,
    }),
  };
}

function lastUpdate(calls: ApiCall[], sessionId: string) {
  return calls.findLast(
    (call) => call.method === "PATCH" && call.path.endsWith(sessionId),
  )?.body;
}

it.skipIf(!POSTGRES_URL)(
  "records every turn on @mastra/pg and replays a reflection from the tape",
  async () => {
    const schemaName = `kitaru_mastra_${randomUUID().replaceAll("-", "")}`;
    const store = new PostgresStore({
      id: "kitaru-postgres-replay",
      connectionString: POSTGRES_URL as string,
      schemaName,
    });
    try {
      await store.init();
      const domain = await store.getStore("memory");
      if (!domain) throw new Error("Missing PostgreSQL memory domain");
      const source = new Memory({
        storage: store,
        options: {
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
        },
      });
      const observer = omModel("observer");
      const reflector = omModel("reflector");
      const actor = new MastraLanguageModelV2Mock({
        modelId: "actor",
        provider: "fixture",
        doStream: async () => textStream("noted"),
      });
      const api = installTestApi();
      const agent = createMemoryReplayAgent(
        ({ memory }) => ({
          id: "postgres-memory",
          name: "Postgres memory",
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

      const turns: { sessionId: string; inputs: Record<string, unknown> }[] =
        [];
      for (let turn = 1; turn <= 6; turn += 1) {
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
        turns.push({
          sessionId,
          inputs: body?.inputs as Record<string, unknown>,
        });
      }

      const envelopes = turns.map(({ inputs }) =>
        decodeMemoryReplayEnvelope(inputs.mastra_memory_replay),
      );
      expect(envelopes[0]?.configuration.memoryStore).toBe("persistent");
      // A later turn must start from a buffered chunk that PostgreSQL returned
      // from its JSON column.
      expect(
        envelopes.some(({ initialSnapshot }) =>
          initialSnapshot.records.some(
            (record) => (record.bufferedObservationChunks?.length ?? 0) > 0,
          ),
        ),
      ).toBe(true);
      const reflectionTurn = envelopes.findIndex(({ omTape }) =>
        omTape?.some(
          (entry) =>
            (entry as { phase?: unknown } | null)?.phase === "reflector",
        ),
      );
      expect(reflectionTurn).toBeGreaterThan(0);
      expect(
        envelopes[reflectionTurn]?.omTape?.some(
          (entry) =>
            (entry as { phase?: unknown } | null)?.phase === "observer",
        ),
      ).toBe(true);

      const observerCalls = observer.doStream.mock.calls.length;
      const reflectorCalls = reflector.doStream.mock.calls.length;
      vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
      vi.stubEnv(
        "KITARU_TASK_INPUTS",
        JSON.stringify(turns[reflectionTurn]?.inputs),
      );
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
      expect(observer.doStream).toHaveBeenCalledTimes(observerCalls);
      expect(reflector.doStream).toHaveBeenCalledTimes(reflectorCalls);
    } finally {
      await store.pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
      await store.close();
    }
  },
  60_000,
);
