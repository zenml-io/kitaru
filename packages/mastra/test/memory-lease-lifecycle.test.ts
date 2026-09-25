import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { InputProcessor } from "@mastra/core/processors";
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
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  REPLAY_ID,
  type TestApi,
} from "./helpers.js";

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
    /** Delay the actor's answer; receives the 1-based actor call number. */
    actorWait?: (call: number) => Promise<void>;
    /** Answer this 1-based actor call with an evidence tool call. */
    callsTool?: (call: number) => boolean;
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
      await options.actorWait?.(actorCalls);
      const callsTool =
        options.callsTool?.(actorCalls) ??
        (options.observe === true && actorCalls % 2 === 1);
      if (!callsTool) return textStream("done");
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
    // A turn without memory selectors fails natively, so it has no text.
    return await output.text.catch(() => undefined);
  }
  /** Replay a recorded baseline from its final session inputs. */
  async function replay(inputs: unknown) {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(inputs));
    try {
      const output = await adapter.stream("ignored");
      await output.consumeStream();
    } finally {
      vi.unstubAllEnvs();
    }
  }
  return { api, memory, domain, turn, replay };
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
    const reason =
      metadata?.mastra_replay_state === "ineligible"
        ? `/${String(metadata.mastra_replay_reason)}`
        : "";
    return `${String(update.body?.status)}/${String(metadata?.mastra_replay_state)}${reason}`;
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
          "completed/ineligible/memory_lease_conflict",
          "completed/ineligible/memory_lease_conflict",
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

const SELECTOR = { threadId: "quick-reply-thread", resourceId: RESOURCE };

it.each(LEASES)(
  "lets a cooperative turn follow a finalizing holder without invalidating it (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const holder = await access.acquire(SELECTOR);
    await holder.markFinalizing?.();
    const successor = await access.acquire(SELECTOR, { cooperative: true });
    expect(successor.overlapsFinalizingTurn).toBe(true);
    expect(await successor.verifyEligibility()).toBe(false);
    expect(await holder.verifyEligibility()).toBe(true);
    // A write the successor registers follows the holder too.
    const write = await access.acquire(SELECTOR, {
      waitMs: 0,
      cooperative: true,
    });
    await write();
    expect(await holder.verifyEligibility()).toBe(true);
    await holder();
    // The successor still holds both selectors after the holder is gone.
    const later = await access.acquire(SELECTOR, { waitMs: 0 });
    expect(await later.verifyEligibility()).toBe(false);
    await later();
    await successor();
    const next = await access.acquire(SELECTOR);
    expect(await next.verifyEligibility()).toBe(true);
    await next();
  },
);

it.each(LEASES)(
  "lets a quick reply follow an earlier reply that is finishing its own memory work (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const holder = await access.acquire(SELECTOR);
    await holder.markFinalizing?.();
    const reply = await access.acquire(SELECTOR, { cooperative: true });
    expect(reply.overlapsFinalizingTurn).toBe(true);
    await reply.markFinalizing?.();
    await holder();
    // Only the ineligible, finalizing reply is still in the way.
    const next = await access.acquire(SELECTOR, { cooperative: true });
    expect(next.overlapsFinalizingTurn).toBe(true);
    await reply();
    await next();
    // Following never poisoned the selectors.
    const later = await access.acquire(SELECTOR);
    expect(await later.verifyEligibility()).toBe(true);
    await later();
  },
);

it.each(LEASES)(
  "lets a quick reply's own writes follow once the earlier turn has released (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const holder = await access.acquire(SELECTOR);
    await holder.markFinalizing?.();
    const reply = await access.acquire(SELECTOR, { cooperative: true });
    expect(reply.overlapsFinalizingTurn).toBe(true);
    // The earlier turn's memory work settles while the reply still answers.
    await holder();
    // Only the reply's own lease is in the way of the write it registers.
    const write = await access.acquire(SELECTOR, {
      waitMs: 0,
      cooperative: true,
    });
    expect(write.overlapsFinalizingTurn).toBe(true);
    await write();
    await reply.markFinalizing?.();
    const next = await access.acquire(SELECTOR, { cooperative: true });
    expect(next.overlapsFinalizingTurn).toBe(true);
    await reply();
    await next();
    // Following never poisoned the selectors.
    const later = await access.acquire(SELECTOR);
    expect(await later.verifyEligibility()).toBe(true);
    await later();
  },
);

it.each(LEASES)(
  "lets a quick reply follow an earlier reply that is still answering (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const holder = await access.acquire(SELECTOR);
    await holder.markFinalizing?.();
    const reply = await access.acquire(SELECTOR, { cooperative: true });
    await holder();
    // Both replies are ineligible already; no eligible turn is in the way.
    const next = await access.acquire(SELECTOR, { cooperative: true });
    expect(next.overlapsFinalizingTurn).toBe(true);
    // A foreign writer still makes it a conflict for every later turn.
    const foreign = await access.acquire(SELECTOR, { waitMs: 0 });
    await foreign();
    const after = await access.acquire(SELECTOR, { cooperative: true });
    expect(after.overlapsFinalizingTurn).toBeFalsy();
    await after();
    await reply();
    await next();
  },
);

it.each(LEASES)(
  "still invalidates a holder for a non-cooperative overlap or before it finalizes (%s lease)",
  async (_kind, createAccess) => {
    const access = await createAccess();
    const running = await access.acquire(SELECTOR);
    const early = await access.acquire(SELECTOR, { cooperative: true });
    expect(early.overlapsFinalizingTurn).toBeFalsy();
    expect(await running.verifyEligibility()).toBe(false);
    await early();
    await running();
    const holder = await access.acquire(SELECTOR);
    await holder.markFinalizing?.();
    const foreign = await access.acquire(SELECTOR, { waitMs: 0 });
    expect(await holder.verifyEligibility()).toBe(false);
    await foreign();
    await holder();
  },
);

/** The final session inputs of the first recorded turn. */
function finalInputs(api: TestApi, sessionId: string): unknown {
  return api.calls.find(
    (call) =>
      call.method === "PATCH" &&
      call.path.endsWith(sessionId) &&
      call.body?.status === "completed",
  )?.body?.inputs;
}

it.each(LEASES)(
  "keeps a turn eligible when a quick reply starts during its buffered observation (%s lease)",
  async (_kind, createAccess) => {
    const observation = gate();
    let observerCalls = 0;
    const thread = `quick-reply-${_kind.replace(" ", "-")}`;
    const { access, marks } = countFinalizingMarks(await createAccess());
    const { api, turn, replay } = await setup(access, {
      thread,
      backgroundObservation: true,
      observerWait: async () => {
        observerCalls += 1;
        await observation.opened;
      },
    });
    expect(await turn(LONG_MESSAGE)).toBe("done");
    await vi.waitFor(() => expect(observerCalls).toBe(1));
    // Kitaru stores the finalizing mark without waiting for it, and the reply
    // must arrive after it lands, as it does at chat pacing.
    await vi.waitFor(() => expect(marks()).toBe(1), { timeout: 5000 });
    const started = Date.now();
    expect(await turn("Quick reply.")).toBe("done");
    expect(Date.now() - started).toBeLessThan(1_000);
    observation.open();
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/ineligible/earlier_turn_finalizing",
        ]),
      { timeout: 5000 },
    );
    const [first] = api.sessionIds;
    expect(memoryMethods(api, String(first))).toContain(
      "updateBufferedObservations",
    );
    const inputs = finalInputs(api, String(first));
    expect(JSON.stringify(inputs)).toContain('"phase":"observer"');
    const recordedObserverCalls = observerCalls;
    await clearBuffering(thread);
    const before = api.calls.length;
    await replay(inputs);
    await vi.waitFor(() =>
      expect(
        api.calls
          .slice(before)
          .some(
            (call) =>
              call.method === "PATCH" && call.body?.status !== undefined,
          ),
      ).toBe(true),
    );
    const closed = api.calls
      .slice(before)
      .filter((call) => call.method === "PATCH" && call.body?.status)
      .at(-1);
    expect(closed?.body).toMatchObject({ status: "completed" });
    expect(
      (closed?.body?.metadata as Record<string, unknown> | undefined)
        ?.mastra_om_divergence,
    ).toBeUndefined();
    // Replay reused the recorded observation instead of calling the observer.
    expect(observerCalls).toBe(recordedObserverCalls);
  },
  15_000,
);

/**
 * Two shared file leases on one directory, one per simulated server. The test
 * picks which server runs each turn, as a load balancer would.
 */
async function createTwoServerAccess(): Promise<ServerAccess> {
  const root = await mkdtemp(join(tmpdir(), "kitaru-lease-two-servers-"));
  roots.push(root);
  const first = createFileMemoryAccess(root);
  const servers = [first, createFileMemoryAccess(root)];
  let current = first;
  const access: MastraExclusiveMemoryAccess = {
    acquire: (selector, options) => current.acquire(selector, options),
    markUnsafeWrite: (selector) => current.markUnsafeWrite(selector),
    resetAfterQuiescence: (selector) => current.resetAfterQuiescence(selector),
  };
  return {
    access,
    useServer: (index) => {
      current = servers[index] ?? current;
    },
  };
}

type ServerAccess = {
  access: MastraExclusiveMemoryAccess;
  useServer: (index: number) => void;
};

/** Count the finalizing marks each lease from `access` has stored. */
function countFinalizingMarks(access: MastraExclusiveMemoryAccess): {
  access: MastraExclusiveMemoryAccess;
  marks: () => number;
} {
  let marks = 0;
  return {
    access: {
      ...access,
      acquire: async (selector, options) => {
        const lease = await access.acquire(selector, options);
        const markFinalizing = lease.markFinalizing;
        if (markFinalizing) {
          lease.markFinalizing = async () => {
            await markFinalizing.call(lease);
            marks += 1;
          };
        }
        return lease;
      },
    },
    marks: () => marks,
  };
}

const QUICK_REPLY_LEASES: Array<[string, () => Promise<ServerAccess>]> = [
  ...LEASES.map(
    ([kind, createAccess]): [string, () => Promise<ServerAccess>] => [
      kind,
      async () => ({ access: await createAccess(), useServer: () => {} }),
    ],
  ),
  ["two-server shared file", createTwoServerAccess],
];

it.each(QUICK_REPLY_LEASES)(
  "keeps a string of quick replies to their own turns once earlier memory work settles mid-reply (%s lease)",
  async (kind, createAccess) => {
    // Chat pacing: each reply arrives a few seconds after the previous answer,
    // while that turn's buffered observation still runs, and the earlier
    // observation finishes while the reply is still answering.
    const firstObservation = gate();
    const replyObservation = gate();
    const observations = [firstObservation, replyObservation];
    const secondAnswer = gate();
    let observerCalls = 0;
    let actorCalls = 0;
    const server = await createAccess();
    const { useServer } = server;
    const { access, marks } = countFinalizingMarks(server.access);
    const thread = `quick-reply-chain-${kind.replaceAll(" ", "-")}`;
    const { api, turn } = await setup(access, {
      thread,
      backgroundObservation: true,
      observerWait: async () => {
        observerCalls += 1;
        await observations[observerCalls - 1]?.opened;
      },
      actorWait: async (call) => {
        actorCalls = call;
        if (call === 2) await secondAnswer.opened;
      },
      // The reply reads evidence, so its next step starts its own buffered
      // observation after the first turn's observation has finished.
      callsTool: (call) => call === 2,
    });
    useServer(0);
    expect(await turn(LONG_MESSAGE)).toBe("done");
    await vi.waitFor(() => expect(observerCalls).toBe(1));
    // Kitaru stores the finalizing mark without waiting for it, and the reply
    // must arrive after it lands, as it does at chat pacing.
    await vi.waitFor(() => expect(marks()).toBe(1), { timeout: 5000 });
    useServer(1);
    const reply = turn(LONG_MESSAGE);
    await vi.waitFor(() => expect(actorCalls).toBe(2));
    // The first turn's observation settles and it releases the selectors.
    firstObservation.open();
    await vi.waitFor(
      () => expect(outcomes(api)[0]).toBe("completed/eligible"),
      { timeout: 5000 },
    );
    // The reply then writes memory and starts its own observation.
    secondAnswer.open();
    expect(await reply).toBe("done");
    await vi.waitFor(() => expect(observerCalls).toBe(2));
    useServer(0);
    expect(await turn("Second quick reply.")).toBe("done");
    replyObservation.open();
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/ineligible/earlier_turn_finalizing",
          "completed/ineligible/earlier_turn_finalizing",
        ]),
      { timeout: 5000 },
    );
    // A turn that starts after every earlier memory write has settled is
    // eligible again.
    useServer(1);
    expect(await turn("After a pause.")).toBe("done");
    await vi.waitFor(
      () => expect(outcomes(api).at(-1)).toBe("completed/eligible"),
      { timeout: 5000 },
    );
  },
  15_000,
);

it.each(LEASES)(
  "still invalidates a finalizing turn when a foreign writer overlaps it (%s lease)",
  async (_kind, createAccess) => {
    const observation = gate();
    let observerStarted = false;
    const access = await createAccess();
    const thread = `foreign-writer-${_kind.replace(" ", "-")}`;
    const { api, turn } = await setup(access, {
      thread,
      backgroundObservation: true,
      observerWait: async () => {
        observerStarted = true;
        await observation.opened;
      },
    });
    expect(await turn(LONG_MESSAGE)).toBe("done");
    await vi.waitFor(() => expect(observerStarted).toBe(true));
    const foreign = await access.acquire(
      { threadId: thread, resourceId: RESOURCE },
      { waitMs: 0 },
    );
    await foreign();
    observation.open();
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/ineligible/memory_lease_conflict",
        ]),
      { timeout: 5000 },
    );
  },
);

/** Clear Mastra's process-wide buffering state, as a replay worker starts. */
async function clearBuffering(thread: string): Promise<void> {
  const scratch = new Memory({
    storage: new InMemoryStore(),
    options: {
      observationalMemory: { model: "fixture/observer", scope: "thread" },
    },
  });
  await (await scratch.omEngine)?.clear(thread, RESOURCE);
}

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
      () =>
        expect(outcomes(api)).toEqual([
          "completed/ineligible/om_settle_timeout",
        ]),
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

/** A turn whose application input processor aborts the run. */
async function trippedTurn(
  hook: "processInput" | "processInputStep",
  captureRequestContext: (context: RequestContext) => Record<string, unknown>,
) {
  const api = installTestApi();
  const runtime = createMemoryRuntime({ messageTokens: 100_000 });
  stores.push(runtime.store);
  await seedMemory(runtime);
  const access = createProcessLocalMemoryAccess();
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("unused"),
  });
  const block = ({ abort }: { abort(reason: string): never }) =>
    abort("Blocked by the guard");
  const guard: InputProcessor =
    hook === "processInput"
      ? { id: "guard", processInput: block }
      : { id: "guard", processInputStep: block };
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "guarded",
      name: "Guarded",
      instructions: "Answer",
      memory,
      model: actor,
      inputProcessors: [guard],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      onRecordingError: () => undefined,
      finalizationWaitMs: 500,
      captureRequestContext,
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: () => actor,
    },
  );
  const requestContext = new RequestContext();
  requestContext.set("tenant", "fixture");
  const output = (await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
    requestContext,
  })) as { consumeStream(): Promise<void>; tripwire: unknown };
  await output.consumeStream();
  expect(output.tripwire).toMatchObject({ reason: "Blocked by the guard" });
  return { access, api };
}

it.each(["processInput", "processInputStep"] as const)(
  "releases the lease and closes the session when an application %s trips",
  async (hook) => {
    const { access, api } = await trippedTurn(hook, (context) =>
      Object.fromEntries(context.entries()),
    );
    const started = Date.now();
    const next = await access.acquire(
      { threadId: THREAD, resourceId: RESOURCE },
      { waitMs: 3_000 },
    );
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await next.verifyEligibility()).toBe(true);
    await next();
    await vi.waitFor(() =>
      expect(outcomes(api)).toEqual(["failed/ineligible/native_run_failed"]),
    );
    const failed = api.calls.find(
      (call) => call.method === "PATCH" && call.body?.status === "failed",
    );
    expect(String(failed?.body?.error)).toMatch(
      /; KITARU_RECORDING_INCOMPLETE:native_run_failed$/,
    );
  },
);

it("closes a native fallback's session when an application processor trips", async () => {
  // A credential-named context key makes the turn fall back to native Mastra.
  const { api } = await trippedTurn("processInput", () => ({
    apiToken: "fixture",
  }));
  await vi.waitFor(() => {
    const update = api.calls.find((call) => call.method === "PATCH");
    expect(update?.body).toMatchObject({
      status: "failed",
      metadata: {
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "credential_key_unsupported",
        mastra_native_state: "failed",
      },
    });
  });
});
