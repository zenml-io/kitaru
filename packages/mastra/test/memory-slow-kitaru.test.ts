import type { InputProcessor } from "@mastra/core/processors";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { Memory } from "@mastra/memory";
import { APICallError } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import type { ResolvedMemoryFile } from "../src/stateful-files.js";
import {
  createMemoryRuntime,
  type MemoryRuntime,
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, type TestApi } from "./helpers.js";

const stores: InMemoryStore[] = [];
const hung: Array<() => void> = [];

afterEach(async () => {
  for (const release of hung.splice(0)) release();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const store of stores.splice(0)) await store.close();
});

type Fetch = typeof globalThis.fetch;
type RequestKind = "session" | "root" | "evidence" | "update";

function kindOf(input: Parameters<Fetch>[0], init?: RequestInit): RequestKind {
  const path = new URL(String(input)).pathname;
  if (init?.method === "PATCH") return "update";
  if (path === "/api/v1/sessions") return "session";
  const nodes = (JSON.parse(String(init?.body)) as { nodes: unknown[] }).nodes;
  return nodes.every(
    (node) => (node as { external_id?: unknown }).external_id === "run",
  )
    ? "root"
    : "evidence";
}

/** Never answer until the request is aborted or the test ends. */
function hang(init?: RequestInit): Promise<Response> {
  return new Promise((_resolve, reject) => {
    const stop = () => reject(new Error("request abandoned"));
    hung.push(stop);
    const signal = init?.signal;
    if (signal?.aborted) reject(signal.reason);
    signal?.addEventListener("abort", () => reject(signal.reason), {
      once: true,
    });
  });
}

const pause = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * A baseline adapter over a live test API whose answers `delay` can hold back.
 *
 * `delay` returns how long to hold a request, or "hang" to never answer.
 */
async function setup(
  options: {
    delay?: (kind: RequestKind) => number | "hang" | undefined;
    files?: readonly string[];
    resolveFile?: (url: string) => Promise<ResolvedMemoryFile>;
    /** Processors that read attachments through the factory's resolver. */
    processors?: (
      resolveFile: (url: string) => Promise<ResolvedMemoryFile>,
    ) => InputProcessor[];
    sourceSettled?: () => Promise<void>;
    sessionSetupWaitMs?: number;
    fileCaptureWaitMs?: number;
    finalizationWaitMs?: number;
    /** Fail the first provider call with a retryable error. */
    failFirstCall?: boolean;
  } = {},
) {
  const api = installTestApi();
  const recorded = globalThis.fetch;
  vi.stubGlobal("fetch", (async (input, init) => {
    const delay = options.delay?.(kindOf(input, init));
    if (delay === "hang") return hang(init);
    if (delay !== undefined) await pause(delay);
    return recorded(input, init);
  }) satisfies Fetch);
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
    },
  });
  const unused = new MastraLanguageModelV2Mock({
    doStream: async () => textStream("unused"),
  });
  await seedMemory({
    store,
    domain,
    memory,
    observer: { calls: [], model: unused },
    reflector: { calls: [], model: unused },
  });
  const modelCalls: number[] = [];
  let startedAt = 0;
  let failedCalls = 0;
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      if (options.failFirstCall && failedCalls++ === 0)
        throw new APICallError({
          message: "Service unavailable",
          url: "https://provider.invalid",
          requestBodyValues: {},
          statusCode: 503,
          isRetryable: true,
        });
      modelCalls.push(Date.now() - startedAt);
      if (modelCalls.length % 2 === 0) return textStream("done");
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: `call-${modelCalls.length}`,
            toolName: "lookup",
            input: "{}",
          },
        ],
        "tool-calls",
      );
    },
  });
  const access = createProcessLocalMemoryAccess();
  const adapter = createMemoryReplayAgent(
    ({ memory, resolveFile }) => ({
      id: "slow-kitaru",
      name: "Slow Kitaru",
      instructions: "Answer",
      memory,
      model: actor,
      tools: {
        lookup: createTool({
          id: "lookup",
          description: "Look something up",
          inputSchema: z.object({}),
          execute: async () => "found",
        }),
      },
      ...(options.processors
        ? { inputProcessors: options.processors(resolveFile) }
        : {}),
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      // Far longer than any wait under test, so only the adapter's own
      // bounds can release the turn.
      timeoutMs: 60_000,
      requestedModelId: "fixture/actor",
      onRecordingError: () => undefined,
      sessionSetupWaitMs: options.sessionSetupWaitMs,
      fileCaptureWaitMs: options.fileCaptureWaitMs,
      finalizationWaitMs: options.finalizationWaitMs,
      files: options.files,
      resolveFile: options.resolveFile,
      sourceMemory: () => ({
        settled: options.sourceSettled ?? (() => memory.settled()),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: () => actor,
    },
  );
  async function turn(message = "Look it up.") {
    startedAt = Date.now();
    const output = (await adapter.stream(message, {
      maxSteps: 5,
      modelSettings: { maxRetries: 1 },
      memory: { thread: THREAD, resource: RESOURCE },
    })) as { consumeStream(): Promise<void>; text: Promise<string> };
    await output.consumeStream();
    return { elapsed: Date.now() - startedAt, text: await output.text };
  }
  return { access, api, modelCalls, turn };
}

/** Each session's closing status, replay state and reason. */
function outcomes(api: TestApi): string[] {
  const created = api.calls.filter(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  );
  return api.sessionIds.map((id, index) => {
    const initial = created[index]?.body?.metadata as
      | Record<string, unknown>
      | undefined;
    const update = api.calls
      .filter(
        (call) =>
          call.method === "PATCH" &&
          call.path.endsWith(id) &&
          call.body?.status !== "in_progress",
      )
      .at(-1);
    // A setup failure reports its reason when it creates the session.
    const metadata = (update?.body?.metadata ?? initial) as
      | Record<string, unknown>
      | undefined;
    return update
      ? `${String(update.body?.status)}/${String(metadata?.mastra_replay_state)}/${String(metadata?.mastra_replay_reason ?? "")}`
      : "pending";
  });
}

it("answers without waiting for hung evidence uploads and closes the session as ineligible", async () => {
  const { api, modelCalls, turn } = await setup({
    delay: (kind) => (kind === "evidence" ? "hang" : undefined),
    finalizationWaitMs: 300,
  });
  const { elapsed, text } = await turn();
  expect(text).toBe("done");
  // The second model call follows the tool step without waiting on its upload.
  expect(modelCalls).toHaveLength(2);
  expect(elapsed).toBeLessThan(1_000);
  await vi.waitFor(
    () =>
      expect(outcomes(api)).toEqual([
        "completed/ineligible/recording_flush_timeout",
      ]),
    { timeout: 3_000 },
  );
});

it("releases the lease before uploading a retried provider attempt", async () => {
  const { access, api, turn } = await setup({
    delay: (kind) => (kind === "evidence" ? "hang" : undefined),
    finalizationWaitMs: 2_000,
    failFirstCall: true,
  });
  const { text } = await turn();
  expect(text).toBe("done");
  const started = Date.now();
  const next = await access.acquire(
    { threadId: THREAD, resourceId: RESOURCE },
    { waitMs: 10_000 },
  );
  // The flush deadline is 4 s; the lease must not wait for it.
  expect(Date.now() - started).toBeLessThan(1_000);
  await next();
  await vi.waitFor(
    () =>
      expect(outcomes(api)).toEqual([
        "completed/ineligible/recording_flush_timeout",
      ]),
    { timeout: 6_000 },
  );
}, 15_000);

it("keeps step order when evidence uploads are slow", async () => {
  const { api, turn } = await setup({
    delay: (kind) => (kind === "evidence" ? 500 : undefined),
  });
  const { elapsed } = await turn();
  expect(elapsed).toBeLessThan(500);
  await vi.waitFor(
    () => expect(outcomes(api)).toEqual(["completed/eligible/"]),
    { timeout: 10_000 },
  );
  const steps = api
    .nodeBatches()
    .flat()
    .filter((node) => node.node_type === "llm_call");
  expect(
    steps.map(
      (node) => (node.outputs as { finish_reason?: unknown }).finish_reason,
    ),
  ).toEqual(["tool-calls", "stop"]);
  const [first, second] = steps.map((node) =>
    Date.parse(String(node.ended_at)),
  );
  // Each step keeps the time it finished, not the time its upload ran.
  expect(Number(second) - Number(first)).toBeLessThan(250);
});

it("answers natively when Kitaru does not open the session in time", async () => {
  const { api, modelCalls, turn } = await setup({
    delay: (kind) => (kind === "session" ? "hang" : undefined),
    sessionSetupWaitMs: 200,
  });
  const { elapsed, text } = await turn();
  expect(text).toBe("done");
  expect(modelCalls).toHaveLength(2);
  expect(modelCalls[0]).toBeLessThan(1_000);
  expect(elapsed).toBeLessThan(1_200);
  expect(api.sessionIds).toEqual([]);
});

it("closes a session that opens after the setup wait as ineligible", async () => {
  const { api, turn } = await setup({
    delay: (kind) => (kind === "session" ? 1_000 : undefined),
    sessionSetupWaitMs: 100,
  });
  const { elapsed, text } = await turn();
  expect(text).toBe("done");
  expect(elapsed).toBeLessThan(800);
  await vi.waitFor(
    () =>
      expect(outcomes(api)).toEqual([
        "completed/ineligible/recording_setup_timeout",
      ]),
    { timeout: 3_000 },
  );
  // The late session never records the native turn's steps.
  expect(
    api
      .nodeBatches()
      .flat()
      .filter((node) => node.external_id !== "run"),
  ).toEqual([]);
});

it("answers natively when a declared file hangs and records why", async () => {
  const REPORT = "https://files.invalid/report.pdf";
  const STALLED = "https://files.invalid/stalled.pdf?token=test-token";
  const downloads: string[] = [];
  const read: number[] = [];
  const { api, turn } = await setup({
    files: [REPORT, STALLED],
    fileCaptureWaitMs: 200,
    resolveFile: (url) => {
      downloads.push(url);
      if (url === STALLED) return new Promise(() => undefined);
      return Promise.resolve({
        bytes: new Uint8Array([1, 2, 3]),
        mediaType: "application/pdf",
      });
    },
    processors: (resolveFile) => [
      {
        id: "attachment-reader",
        async processInput({ messages }) {
          // This turn needs only the attachment that downloads.
          read.push((await resolveFile(REPORT)).bytes.length);
          return messages;
        },
      },
    ],
  });
  const { elapsed, text } = await turn();
  expect(text).toBe("done");
  expect(elapsed).toBeLessThan(1_200);
  expect(read).toEqual([3]);
  // The native turn reuses the capture's download instead of a second fetch.
  expect(downloads).toEqual([REPORT, STALLED]);
  await vi.waitFor(() =>
    expect(outcomes(api)).toEqual([
      "completed/ineligible/file_capture_timeout",
    ]),
  );
  expect(JSON.stringify(api.calls)).not.toContain("test-token");
});

it("hands a running file download to the native turn instead of fetching it again", async () => {
  const FILE = "https://files.invalid/slow.pdf";
  let downloads = 0;
  const read: number[] = [];
  const { api, turn } = await setup({
    files: [FILE],
    fileCaptureWaitMs: 50,
    resolveFile: async () => {
      downloads += 1;
      await pause(300);
      return { bytes: new Uint8Array([7]), mediaType: "application/pdf" };
    },
    processors: (resolveFile) => [
      {
        id: "attachment-reader",
        async processInput({ messages }) {
          read.push((await resolveFile(FILE)).bytes[0] ?? -1);
          return messages;
        },
      },
    ],
  });
  const { elapsed, text } = await turn();
  expect(text).toBe("done");
  expect(read).toEqual([7]);
  expect(downloads).toBe(1);
  // The download started before the wait ran out, so it ends at about 300 ms.
  expect(elapsed).toBeLessThan(1_000);
  await vi.waitFor(() =>
    expect(outcomes(api)).toEqual([
      "completed/ineligible/file_capture_timeout",
    ]),
  );
});

it("starts a recorded turn without waiting for other threads' source memory work", async () => {
  const { api, modelCalls, turn } = await setup({
    // Another feature's background work on the shared source Memory.
    sourceSettled: () => pause(30_000),
  });
  await turn();
  expect(modelCalls[0]).toBeLessThan(1_000);
  await vi.waitFor(
    () => expect(outcomes(api)).toEqual(["completed/eligible/"]),
    { timeout: 3_000 },
  );
});

/**
 * Register an async buffered observation on THREAD that an earlier turn left
 * running, in Mastra's process-wide buffering map.
 */
async function holdBuffering(runtime: MemoryRuntime): Promise<void> {
  const engine = await runtime.memory.omEngine;
  if (!engine) throw new Error("Missing observational-memory engine");
  const ops = (
    engine.buffering.constructor as unknown as {
      asyncBufferingOps: Map<string, Promise<void>>;
    }
  ).asyncBufferingOps;
  const key = engine.buffering.getObservationBufferKey(
    engine.buffering.getLockKey(THREAD, RESOURCE),
  );
  const running = new Promise<void>((resolve) => hung.push(resolve));
  ops.set(key, running);
  void running.then(() => ops.delete(key));
}

async function bufferingSetup(options: { kitaruDown?: boolean }) {
  const api = installTestApi();
  const recorded = globalThis.fetch;
  vi.stubGlobal("fetch", (async (input, init) => {
    if (options.kitaruDown && init?.method !== "PATCH")
      throw new TypeError("fetch failed");
    return recorded(input, init);
  }) satisfies Fetch);
  const runtime = createMemoryRuntime();
  stores.push(runtime.store);
  await seedMemory(runtime);
  const access = createProcessLocalMemoryAccess();
  let startedAt = 0;
  const modelCalls: number[] = [];
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      modelCalls.push(Date.now() - startedAt);
      return textStream("done");
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "buffering",
      name: "Buffering",
      instructions: "Answer",
      memory,
      model: actor,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      timeoutMs: 60_000,
      requestedModelId: "fixture/actor",
      onRecordingError: () => undefined,
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: () => actor,
    },
  );
  async function turn() {
    startedAt = Date.now();
    const output = (await adapter.stream("Hello", {
      memory: { thread: THREAD, resource: RESOURCE },
    })) as { consumeStream(): Promise<void>; text: Promise<string> };
    await output.consumeStream();
    return await output.text;
  }
  return { access, api, modelCalls, runtime, turn };
}

it("answers natively without waiting for an earlier turn's buffered observation", async () => {
  const { access, modelCalls, runtime, turn } = await bufferingSetup({
    kitaruDown: true,
  });
  // The earlier turn still holds its lease while its observation runs.
  const earlier = await access.acquire({
    threadId: THREAD,
    resourceId: RESOURCE,
  });
  await holdBuffering(runtime);
  expect(await turn()).toBe("done");
  expect(modelCalls[0]).toBeLessThan(1_000);
  await earlier();
});

it("records a turn as ineligible instead of waiting for buffering an earlier turn left running", async () => {
  const { api, modelCalls, runtime, turn } = await bufferingSetup({});
  await holdBuffering(runtime);
  expect(await turn()).toBe("done");
  expect(modelCalls[0]).toBeLessThan(1_000);
  await vi.waitFor(
    () =>
      expect(outcomes(api)).toEqual(["completed/ineligible/om_work_unjoined"]),
    { timeout: 3_000 },
  );
});
