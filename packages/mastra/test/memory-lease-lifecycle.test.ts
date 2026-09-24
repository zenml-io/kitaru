import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from "@mastra/core/request-context";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { Memory } from "@mastra/memory";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  type MastraExclusiveMemoryAccess,
} from "../src/memory.js";
import { createFileMemoryAccess } from "./helpers/file-memory-access.js";
import {
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, type TestApi } from "./helpers.js";

const roots: string[] = [];
const stores: InMemoryStore[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const store of stores.splice(0)) await store.close();
  for (const root of roots.splice(0))
    await rm(root, { recursive: true, force: true, maxRetries: 5 });
});

const LONG_MESSAGE = `Observe this: ${"The user prefers green for every report. ".repeat(300)}`;

const pause = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

const LEASES: Array<[string, () => Promise<MastraExclusiveMemoryAccess>]> = [
  ["process-local", async () => createProcessLocalMemoryAccess()],
  [
    "shared file",
    async () => {
      const root = await mkdtemp(join(tmpdir(), "kitaru-lease-lifecycle-"));
      roots.push(root);
      return createFileMemoryAccess(root);
    },
  ],
];

function gate() {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

function omModel(kind: "observer" | "reflector", wait?: () => Promise<void>) {
  const text =
    kind === "observer"
      ? `OBSERVED: ${"The user changed the preference to green. ".repeat(15)}`
      : "REFLECTED: the user prefers green.";
  return new MastraLanguageModelV2Mock({
    modelId: kind,
    provider: "fixture",
    doStream: async () => {
      await wait?.();
      return textStream(
        `<observations>\n${text}\n</observations>\n<current-task>Continue.</current-task>`,
      );
    },
  });
}

/** A source Memory whose first evidence-heavy turn starts a buffered reflection. */
async function setup(
  access: MastraExclusiveMemoryAccess,
  options: {
    observerWait?: () => Promise<void>;
    reflectorWait?: () => Promise<void>;
    observe?: boolean;
    /** Observe in the background: buffer without reaching the threshold. */
    backgroundObservation?: boolean;
    finalizationWaitMs?: number;
    joinSourceMemory?: boolean;
    /**
     * Use a fresh thread. Mastra keeps buffering state per thread for the whole
     * process, so background observation needs a thread no other test used.
     */
    thread?: string;
    /** Wrap the test API before the adapter's client captures fetch. */
    wrapFetch?: (fetch: typeof globalThis.fetch) => typeof globalThis.fetch;
  } = {},
) {
  const api = installTestApi();
  if (options.wrapFetch)
    vi.stubGlobal("fetch", options.wrapFetch(globalThis.fetch));
  const observer = omModel("observer", options.observerWait);
  const reflector = omModel("reflector", options.reflectorWait);
  const store = new InMemoryStore();
  stores.push(store);
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      semanticRecall: false,
      workingMemory: {
        enabled: true,
        scope: "thread",
        schema: z.object({ preference: z.string() }),
      },
      observationalMemory: {
        scope: "thread",
        observation: {
          model: observer,
          messageTokens: options.backgroundObservation
            ? 10_000
            : options.observe
              ? 300
              : 1_000_000,
          bufferTokens: 0.2,
          bufferActivation: 1,
          blockAfter: 1.1,
        },
        reflection: {
          model: reflector,
          observationTokens: 200,
          bufferActivation: 0.5,
          blockAfter: 1.1,
        },
      },
    },
  });
  const thread = options.thread ?? THREAD;
  if (options.thread)
    await memory.createThread({ threadId: thread, resourceId: RESOURCE });
  else
    await seedMemory({
      store,
      domain,
      memory,
      observer: { calls: [], model: observer },
      reflector: { calls: [], model: reflector },
    });
  let actorCalls = 0;
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      actorCalls += 1;
      if (!options.observe || actorCalls % 2 === 0) return textStream("done");
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: `call-${actorCalls}`,
            toolName: "evidence",
            input: "{}",
          },
        ],
        "tool-calls",
      );
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "lease-lifecycle",
      name: "Lease lifecycle",
      instructions: "Answer",
      memory,
      model: actor,
      tools: {
        evidence: createTool({
          id: "evidence",
          description: "Read evidence",
          inputSchema: z.object({}),
          execute: async () => "The user now prefers green. ".repeat(60),
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      finalizationWaitMs: options.finalizationWaitMs,
      onRecordingError: () => undefined,
      sourceMemory: () => ({
        settled: () => memory.settled(),
        ...(options.joinSourceMemory ? { memory } : {}),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: (id) =>
        id.endsWith("observer")
          ? observer
          : id.endsWith("reflector")
            ? reflector
            : actor,
    },
  );
  async function turn(
    message: string,
    selector: { thread: string; resource?: string } | null = {
      thread,
      resource: RESOURCE,
    },
    requestContext?: RequestContext,
  ) {
    const output = await adapter.stream(message, {
      maxSteps: 5,
      ...(selector ? { memory: selector } : {}),
      ...(requestContext ? { requestContext } : {}),
    });
    await output.consumeStream();
  }
  return { api, memory, domain, turn };
}

/** The final replay state of every session, in creation order. */
function outcomes(api: TestApi): string[] {
  const created = api.calls.filter(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  );
  return api.sessionIds.map((id, index) => {
    const initial = created[index]?.body?.metadata as
      | Record<string, unknown>
      | undefined;
    // A setup failure reports its own session after the native answer.
    if (initial?.mastra_replay_state === "ineligible") return "setup-failure";
    const update = api.calls
      .filter(
        (call) =>
          call.method === "PATCH" &&
          call.path.endsWith(id) &&
          call.body?.status !== "in_progress",
      )
      .at(-1);
    if (!update) return "pending";
    const metadata = update.body?.metadata as
      | Record<string, unknown>
      | undefined;
    return `${String(update.body?.status)}/${String(metadata?.mastra_replay_state)}`;
  });
}

function memoryMethods(api: TestApi, sessionId: string): unknown[] {
  return api
    .nodeBatches(sessionId)
    .flat()
    .filter((node) => node.name === "memory_mutation")
    .map((node) => (node.attributes as Record<string, unknown>).memory_method);
}

it.each(LEASES)(
  "records two turns sent 100 ms apart on one thread as eligible (%s lease)",
  async (_kind, createAccess) => {
    const { api, turn } = await setup(await createAccess(), {
      // A slow session update must not keep the thread leased.
      wrapFetch: (recorded) => async (input, init) => {
        if (init?.method === "PATCH") await pause(300);
        return recorded(input, init);
      },
    });
    await turn("Turn one.");
    await pause(100);
    await turn("Turn two.");
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/eligible",
        ]),
      { timeout: 3000 },
    );
  },
);

it.each(LEASES)(
  "records a late buffered reflection in its own turn and keeps the next turn eligible (%s lease)",
  async (_kind, createAccess) => {
    const reflection = gate();
    let reflectorStarted = false;
    const { api, turn } = await setup(await createAccess(), {
      observe: true,
      reflectorWait: async () => {
        reflectorStarted = true;
        await reflection.opened;
      },
    });
    await turn("Remember the evidence.");
    await vi.waitFor(() => expect(reflectorStarted).toBe(true));
    // The stream has closed; the reflection finishes well after it.
    await pause(300);
    expect(outcomes(api)).toEqual(["pending"]);
    reflection.open();
    await vi.waitFor(
      () => expect(outcomes(api)).toEqual(["completed/eligible"]),
      {
        timeout: 3000,
      },
    );
    const [first] = api.sessionIds;
    expect(memoryMethods(api, String(first))).toContain(
      "updateBufferedReflection",
    );
    const finalInputs = JSON.stringify(
      api.calls.find(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      )?.body?.inputs,
    );
    expect(finalInputs).toContain('"phase":"reflector"');
    await turn("Next turn.");
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/eligible",
        ]),
      { timeout: 3000 },
    );
  },
);

it.each(LEASES)(
  "keeps turns after a native fallback turn eligible (%s lease)",
  async (_kind, createAccess) => {
    let failed = false;
    const { api, turn } = await setup(await createAccess(), {
      wrapFetch: (recorded) => async (input, init) => {
        if (
          !failed &&
          init?.method === "POST" &&
          new URL(String(input)).pathname === "/api/v1/sessions"
        ) {
          failed = true;
          throw new TypeError("Kitaru is unavailable");
        }
        return recorded(input, init);
      },
    });
    await turn("Answered natively.");
    await turn("Same thread.");
    await turn("Other thread.", { thread: "other-thread", resource: RESOURCE });
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/eligible",
        ]),
      { timeout: 3000 },
    );
  },
);

it.each(LEASES)(
  "does not let a call without memory selectors affect other threads (%s lease)",
  async (_kind, createAccess) => {
    const { api, turn } = await setup(await createAccess());
    await turn("Summarize this without memory.", null);
    await turn("A normal turn.");
    await vi.waitFor(
      () =>
        expect(outcomes(api).sort()).toEqual([
          "completed/eligible",
          "setup-failure",
        ]),
      { timeout: 3000 },
    );
  },
);

it.each(LEASES)(
  "records two turns 100 ms apart as eligible while every Kitaru request is slow (%s lease)",
  async (_kind, createAccess) => {
    const { api, turn } = await setup(await createAccess(), {
      // Evidence uploads after the stream closes must not keep the thread leased.
      wrapFetch: (recorded) => async (input, init) => {
        await pause(250);
        return recorded(input, init);
      },
    });
    await turn("Turn one.");
    await pause(100);
    await turn("Turn two.");
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/eligible",
        ]),
      { timeout: 5000 },
    );
  },
  15_000,
);

it.each(LEASES)(
  "does not let a call whose selectors come from the request context affect other threads (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const markUnsafeWrite = vi.spyOn(access, "markUnsafeWrite");
    const { api, domain, turn } = await setup(access);
    const resourceOnly = new RequestContext();
    resourceOnly.set(MASTRA_RESOURCE_ID_KEY, "context-resource");
    await turn(
      "Thread in options, resource in context.",
      { thread: "context-thread-a" },
      resourceOnly,
    );
    const both = new RequestContext();
    both.set(MASTRA_THREAD_ID_KEY, "context-thread-b");
    both.set(MASTRA_RESOURCE_ID_KEY, "context-resource");
    await turn("Both selectors in context.", null, both);
    await turn("An unrelated user.", {
      thread: "unrelated-thread",
      resource: "unrelated-resource",
    });
    await vi.waitFor(
      () =>
        expect(outcomes(api).sort()).toEqual([
          "completed/eligible",
          "setup-failure",
          "setup-failure",
        ]),
      { timeout: 3000 },
    );
    expect(markUnsafeWrite).not.toHaveBeenCalled();
    for (const threadId of ["context-thread-a", "context-thread-b"]) {
      const { messages } = await domain.listMessages({ threadId });
      expect(messages.length).toBeGreaterThan(0);
      expect(
        messages.every((message) => message.resourceId === "context-resource"),
      ).toBe(true);
    }
  },
);

it.each(LEASES)(
  "keeps a thread ineligible after an unregistered writer until quiescence reset (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const { api, turn } = await setup(access);
    await access.markUnsafeWrite({ threadId: THREAD, resourceId: RESOURCE });
    await turn("After a foreign write.");
    await turn("Other thread, same resource.", {
      thread: "other-thread",
      resource: RESOURCE,
    });
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "failed/ineligible",
          "failed/ineligible",
        ]),
      { timeout: 3000 },
    );
    await access.resetAfterQuiescence({
      threadId: THREAD,
      resourceId: RESOURCE,
    });
    await turn("After the application reset.");
    await vi.waitFor(() =>
      expect(outcomes(api).at(-1)).toBe("completed/eligible"),
    );
  },
);

it("releases the lease and closes the session when OM work misses the finalization deadline", async () => {
  const access = createProcessLocalMemoryAccess();
  const hung = gate();
  const { api, turn } = await setup(access, {
    thread: "hung-observer-thread",
    backgroundObservation: true,
    finalizationWaitMs: 200,
    observerWait: () => hung.opened,
  });
  try {
    await turn(LONG_MESSAGE);
    await vi.waitFor(
      () => expect(outcomes(api)).toEqual(["failed/ineligible"]),
      { timeout: 3000 },
    );
    const probe = await access.acquire({
      threadId: "other-thread",
      resourceId: RESOURCE,
    });
    expect(await probe.verifyEligibility()).toBe(true);
    await probe();
  } finally {
    hung.open();
  }
});

it("lets the source Memory's settled() join a turn's observational-memory work", async () => {
  let observed = false;
  const { memory, turn } = await setup(createProcessLocalMemoryAccess(), {
    thread: "source-settled-thread",
    backgroundObservation: true,
    joinSourceMemory: true,
    observerWait: async () => {
      await pause(300);
      observed = true;
    },
  });
  await turn(LONG_MESSAGE);
  expect(observed).toBe(false);
  await memory.settled();
  expect(observed).toBe(true);
});

it("bounds the source Memory's settled() by the finalization deadline when OM work hangs", async () => {
  const hung = gate();
  const { memory, turn } = await setup(createProcessLocalMemoryAccess(), {
    thread: "hung-settled-thread",
    backgroundObservation: true,
    joinSourceMemory: true,
    finalizationWaitMs: 200,
    observerWait: () => hung.opened,
  });
  try {
    await turn(LONG_MESSAGE);
    const outcome = await Promise.race([
      memory.settled().then(() => "settled"),
      pause(3000).then(() => "still waiting"),
    ]);
    expect(outcome).toBe("settled");
  } finally {
    hung.open();
  }
});
