import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { Memory } from "@mastra/memory";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

const THREAD = "fidelity-thread";
const RESOURCE = "fidelity-owner";
const ACTOR_URL = "https://actor.invalid/v1";
const DAY_MS = 86_400_000;
const BASELINE_AT = new Date("2026-09-01T09:00:00.000Z").getTime();

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

/** Rebuild objects in PostgreSQL `jsonb` key order: shorter keys first, then bytes. */
function jsonbOrder(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(jsonbOrder);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(
        ([left], [right]) =>
          Buffer.byteLength(left) - Buffer.byteLength(right) ||
          Buffer.compare(Buffer.from(left), Buffer.from(right)),
      )
      .map(([key, item]) => [key, jsonbOrder(item)]),
  );
}

function completionStream(text: string): Response {
  const chunk = (choice: Record<string, unknown>, extra = {}) =>
    `data: ${JSON.stringify({
      id: "completion",
      object: "chat.completion.chunk",
      created: 0,
      model: "actor",
      choices: [{ index: 0, ...choice }],
      ...extra,
    })}\n\n`;
  return new Response(
    chunk({
      delta: { role: "assistant", content: text },
      finish_reason: null,
    }) +
      chunk(
        { delta: {}, finish_reason: "stop" },
        {
          usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 },
        },
      ) +
      "data: [DONE]\n\n",
    { headers: { "Content-Type": "text/event-stream" } },
  );
}

function day(ms: number): string {
  return new Date(ms).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

async function seedSource() {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing memory store");
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 10,
      workingMemory: {
        enabled: true,
        scope: "thread",
        schema: z.object({
          favoriteColor: z.string(),
          name: z.string().optional(),
          city: z.string().optional(),
        }),
      },
      observationalMemory: {
        scope: "thread",
        activateAfterIdle: "2m",
        observation: {
          model: "fixture/observer",
          messageTokens: 100_000,
          bufferTokens: 50_000,
        },
        reflection: { model: "fixture/reflector", observationTokens: 100_000 },
      },
    },
  });
  await memory.createThread({
    threadId: THREAD,
    resourceId: RESOURCE,
    title: "Order",
  });
  await memory.saveMessages({
    messages: [
      {
        id: "question",
        role: "user",
        content: {
          format: 2,
          parts: [{ type: "text", text: "Where is my order A-1?" }],
        },
        createdAt: new Date(BASELINE_AT - 3 * 60_000),
        threadId: THREAD,
        resourceId: RESOURCE,
      },
      {
        id: "answer",
        role: "assistant",
        content: {
          format: 2,
          parts: [
            {
              type: "tool-invocation",
              toolInvocation: {
                state: "result",
                toolCallId: "call-1",
                toolName: "lookupOrder",
                args: { orderNumber: "A-1", zip: "2611" },
                result: { status: "shipped", eta: "tomorrow" },
              },
            },
            { type: "text", text: "It shipped and arrives tomorrow." },
          ],
        },
        createdAt: new Date(BASELINE_AT - 60_000),
        threadId: THREAD,
        resourceId: RESOURCE,
      },
    ],
  });
  const record = await domain.initializeObservationalMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    scope: "thread",
    config: {},
  });
  await domain.updateActiveObservations({
    id: record.id,
    observations: `Date: ${day(BASELINE_AT)}\n* 🔴 (09:00) User has a dentist appointment tomorrow. (meaning ${day(BASELINE_AT + DAY_MS)})`,
    tokenCount: 30,
    lastObservedAt: new Date(BASELINE_AT - 10 * 60_000),
    observedMessageIds: [],
  });
  // Mastra activates this chunk only once the last answer is idle for 2m.
  await domain.updateBufferedObservations({
    id: record.id,
    chunk: {
      cycleId: "cycle-1",
      observations: `Date: ${day(BASELINE_AT)}\n* 🔴 (08:59) BUFFERED_ONLY: User asked about order A-1.`,
      tokenCount: 15,
      messageIds: ["question", "answer"],
      messageTokens: 40,
      lastObservedAt: new Date(BASELINE_AT - 60_000),
    },
  });
  return { store, domain, memory };
}

it("replays the baseline's exact first request after storage re-sorts keys and weeks pass", async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(BASELINE_AT);
  const source = await seedSource();
  const api = installTestApi();
  const kitaruFetch = globalThis.fetch;
  const bodies: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof globalThis.fetch>(async (input, init) => {
      if (String(input).startsWith(ACTOR_URL)) {
        bodies.push(String(init?.body));
        return completionStream("It shipped.");
      }
      return kitaruFetch(input, init);
    }),
  );
  const unexpected = async (): Promise<never> => {
    throw new Error("Replay must reuse recorded memory results.");
  };
  const actor = {
    id: "custom/actor" as const,
    url: ACTOR_URL,
    apiKey: "test-key",
  };
  const agent = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "fidelity",
      name: "Fidelity",
      instructions: "Answer in one short sentence.",
      model: actor,
      memory,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "custom/actor",
      sourceMemory: () => ({
        settled: () => source.memory.settled(),
        domain: source.domain,
        configuration: source.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: (id) =>
        id === "custom/actor"
          ? actor
          : new MastraLanguageModelV2Mock({
              modelId: id,
              provider: "fixture",
              doStream: unexpected,
            }),
    },
  );
  try {
    const baseline = await agent.stream("What is my favorite color?", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await baseline.consumeStream();
    const eligible = () =>
      api.calls.find(
        (call) =>
          call.method === "PATCH" &&
          (call.body?.metadata as Record<string, unknown> | undefined)
            ?.mastra_replay_state === "eligible",
      );
    await vi.waitFor(() => expect(eligible()).toBeDefined());
    const recorded = bodies[0];
    expect(recorded).toContain('\\"orderNumber\\":\\"A-1\\",\\"zip\\"');
    expect(recorded).toContain(
      `meaning ${day(BASELINE_AT + DAY_MS)} - tomorrow`,
    );
    expect(recorded).not.toContain("BUFFERED_ONLY");

    const stored = jsonbOrder(eligible()?.body?.inputs);
    expect(JSON.stringify(stored)).not.toBe(
      JSON.stringify(eligible()?.body?.inputs),
    );
    vi.setSystemTime(BASELINE_AT + 21 * DAY_MS);
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(stored));
    const replayFrom = bodies.length;
    const replay = await agent.stream("ignored");
    await replay.consumeStream();
    expect(await replay.text).toBe("It shipped.");
    expect(bodies[replayFrom]).toBe(recorded);
    // Only Mastra's own time checks see the recorded clock.
    expect(Date.now()).toBe(BASELINE_AT + 21 * DAY_MS);
  } finally {
    await source.memory.settled();
    await source.store.close();
  }
});
