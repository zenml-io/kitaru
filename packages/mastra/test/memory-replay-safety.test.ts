import { createHash } from "node:crypto";
import { Agent } from "@mastra/core/agent";
import {
  MASTRA_AUTH_TOKEN_KEY,
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from "@mastra/core/request-context";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import { createProcessLocalMemoryAccess } from "../src/memory-binding.js";
import {
  createMemoryReplayEnvelope,
  decodeMemoryReplayEnvelope,
  type MastraMemoryReplayInput,
} from "../src/memory-snapshot.js";
import {
  createMemoryReplayAgent,
  type MemoryReplayAgentFactory,
  type MemoryReplayAgentOptions,
} from "../src/stateful-agent.js";
import { textStream } from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

const stores: InMemoryStore[] = [];
afterEach(async () => {
  for (const store of stores.splice(0)) await store.close();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

function input(): MastraMemoryReplayInput {
  return {
    invocationId: "safety-test",
    rawInput: "hello",
    initialSnapshot: {
      threadId: "thread",
      resourceId: "resource",
      thread: null,
      resource: null,
      messages: [],
      records: [],
    },
    configuration: {
      runOptions: { memory: { thread: "thread", resource: "resource" } },
    },
    requestContext: {},
    files: [],
  };
}

it("stores captured file references and refuses signed source URLs", () => {
  const bytes = new Uint8Array([1, 2, 3]);
  const digest = createHash("sha256")
    .update("image/png")
    .update("\0")
    .update(bytes)
    .digest("hex");
  const ref = `kitaru-file://sha256/${digest}`;
  const safe = input();
  safe.rawInput = { file: new URL(ref) };
  safe.files = [{ url: ref, mediaType: "image/png", bytes }];
  const envelope = createMemoryReplayEnvelope(safe);
  expect(envelope.complete, envelope.reasons.join("; ")).toBe(true);
  expect(decodeMemoryReplayEnvelope(envelope).files[0]?.bytes).toEqual(bytes);

  const plain = createMemoryReplayEnvelope({
    ...safe,
    files: [
      { url: "https://files.invalid/image.png", mediaType: "image/png", bytes },
    ],
  });
  expect(plain.complete).toBe(false);

  const signed = createMemoryReplayEnvelope({
    ...safe,
    rawInput: {
      file: "https://files.invalid/image.png?X-Amz-Signature=SECRET",
    },
  });
  expect(signed.complete).toBe(false);
  expect(JSON.stringify(signed)).not.toContain("SECRET");

  const captured = envelope.files[0];
  if (!captured) throw new Error("Missing captured file");
  const altered = {
    ...envelope,
    files: [{ ...captured, url: `kitaru-file://sha256/${"0".repeat(64)}` }],
  };
  expect(() => decodeMemoryReplayEnvelope(altered)).toThrow(
    /captured content reference/,
  );
});

/** The native fallback registers each write; no recording leased the thread. */
function expectOnlyWriteRegistrations(acquire: {
  mock: { calls: unknown[][] };
}): void {
  expect(acquire.mock.calls.length).toBeGreaterThan(0);
  for (const [, options] of acquire.mock.calls)
    expect(options).toMatchObject({ waitMs: 0 });
}

function fixture(
  factory?: MemoryReplayAgentFactory,
  overrides: Partial<MemoryReplayAgentOptions> = {},
) {
  const store = new InMemoryStore();
  stores.push(store);
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  const modelCall = vi.fn(async () => textStream("done"));
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: modelCall,
  });
  const lease = createProcessLocalMemoryAccess();
  const acquire = vi.spyOn(lease, "acquire");
  const adapter = createMemoryReplayAgent(
    factory ??
      (({ memory }) => ({
        id: "safety",
        name: "Safety",
        memory,
        model,
        instructions: "Answer",
      })),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: async () => {},
        domain,
        configuration: { semanticRecall: false },
        exclusiveAccess: lease,
      }),
      resolveModel: () => model,
      ...overrides,
    },
  );
  return { adapter, modelCall, acquire, lease };
}

it.each(["defaultOptions", "runOptions", "memoryConfig"])(
  "excludes transport credentials from %s during capture and decode",
  (key) => {
    const original = input();
    const unsafe = {
      providerOptions: {
        openai: { websocket: { headers: { "x-custom-access": "CREDENTIAL" } } },
      },
    };
    const clean = createMemoryReplayEnvelope(original);
    const envelope = createMemoryReplayEnvelope({
      ...original,
      configuration: { ...original.configuration, [key]: unsafe },
    });
    expect(envelope.complete).toBe(false);
    expect(JSON.stringify(envelope)).not.toContain("CREDENTIAL");
    expect(() =>
      decodeMemoryReplayEnvelope({
        ...clean,
        configuration: { ...original.configuration, [key]: unsafe },
      }),
    ).toThrow(/transport|sensitive key/i);
  },
);

it("rejects native auth tokens during capture and decode", () => {
  const original = input();
  const requestContext = { [MASTRA_AUTH_TOKEN_KEY]: "CREDENTIAL" };
  const clean = createMemoryReplayEnvelope(original);
  const unsafe = createMemoryReplayEnvelope({ ...original, requestContext });
  expect(unsafe.complete).toBe(false);
  expect(JSON.stringify(unsafe)).not.toContain("CREDENTIAL");
  expect(() =>
    decodeMemoryReplayEnvelope({ ...clean, requestContext }),
  ).toThrow(/auth/i);
});

it("keeps native auth context while marking uncaptured context ineligible", async () => {
  const api = installTestApi();
  const { adapter, modelCall } = fixture();
  const requestContext = new RequestContext();
  requestContext.set(MASTRA_AUTH_TOKEN_KEY, "CREDENTIAL");
  const result = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
    requestContext,
  });
  await result.consumeStream();
  expect(await result.text).toBe("done");
  expect(modelCall).toHaveBeenCalledTimes(1);
  await vi.waitFor(() =>
    expect(
      api.calls.find(
        (call) => call.method === "PATCH" && call.body?.status === "failed",
      )?.body?.metadata,
    ).toMatchObject({ mastra_replay_state: "ineligible" }),
  );
  expect(JSON.stringify(api.calls)).not.toContain("CREDENTIAL");
});

it.each([MASTRA_THREAD_ID_KEY, MASTRA_RESOURCE_ID_KEY])(
  "keeps a mismatched %s native and rejects its recording before leasing",
  async (key) => {
    const api = installTestApi();
    const { adapter, modelCall, acquire } = fixture();
    const requestContext = new RequestContext();
    requestContext.set(key, "other");
    const result = await adapter.stream("hello", {
      memory: { thread: "thread", resource: "resource" },
      requestContext,
    });
    await result.consumeStream();
    expect(await result.text).toBe("done");
    expectOnlyWriteRegistrations(acquire);
    expect(modelCall).toHaveBeenCalledTimes(1);
    await vi.waitFor(() =>
      expect(
        api.calls.find(
          (call) => call.method === "POST" && call.path === "/api/v1/sessions",
        )?.body?.metadata,
      ).toMatchObject({
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "context_unsupported",
      }),
    );
  },
);

it("does not make mismatched middleware selectors replayable through selective capture", async () => {
  const api = installTestApi();
  const { adapter, acquire } = fixture(undefined, {
    captureRequestContext: () => ({ locale: "en" }),
  });
  const requestContext = new RequestContext();
  requestContext.set(MASTRA_THREAD_ID_KEY, "authorized-thread");
  const result = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
    requestContext,
  });
  await result.consumeStream();
  expect(await result.text).toBe("done");
  expectOnlyWriteRegistrations(acquire);
  await vi.waitFor(() =>
    expect(
      api.calls.find(
        (call) => call.method === "POST" && call.path === "/api/v1/sessions",
      )?.body?.metadata,
    ).toMatchObject({
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "context_unsupported",
    }),
  );
});

it("allows selective capture to exclude live authentication tokens", async () => {
  const api = installTestApi();
  const { adapter } = fixture(undefined, {
    captureRequestContext: () => ({ locale: "en" }),
  });
  const requestContext = new RequestContext();
  requestContext.set(MASTRA_AUTH_TOKEN_KEY, "CREDENTIAL");
  const result = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
    requestContext,
  });
  await result.consumeStream();
  expect(JSON.stringify(api.calls)).not.toContain("CREDENTIAL");
  await vi.waitFor(() =>
    expect(
      api.calls.find(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      )?.body?.metadata,
    ).toMatchObject({ mastra_native_state: "completed" }),
  );
});

it("records and replays matching middleware and invocation memory selectors", async () => {
  const api = installTestApi();
  const { adapter, modelCall, acquire } = fixture(undefined, {
    captureRequestContext: (context) => ({
      [MASTRA_THREAD_ID_KEY]: context.get(MASTRA_THREAD_ID_KEY),
      [MASTRA_RESOURCE_ID_KEY]: context.get(MASTRA_RESOURCE_ID_KEY),
    }),
  });
  const requestContext = new RequestContext();
  requestContext.set(MASTRA_THREAD_ID_KEY, "thread");
  requestContext.set(MASTRA_RESOURCE_ID_KEY, "resource");
  const baseline = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
    requestContext,
  });
  await baseline.consumeStream();
  const recorded = api.calls.find((call) => call.path === "/api/v1/sessions")
    ?.body?.inputs;
  expect(recorded).toHaveProperty("mastra_memory_replay.complete", true);
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(recorded));
  const replay = await adapter.stream("ignored", {
    memory: { thread: "live-thread", resource: "live-resource" },
  });
  await replay.consumeStream();
  expect(modelCall).toHaveBeenCalledTimes(2);
  expect(acquire).toHaveBeenCalledTimes(1);
  await vi.waitFor(() =>
    expect(
      api.calls.filter(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toHaveLength(2),
  );
});

it.each([MASTRA_THREAD_ID_KEY, MASTRA_RESOURCE_ID_KEY, MASTRA_AUTH_TOKEN_KEY])(
  "preserves native resolver mutations of %s while making recording ineligible",
  async (key) => {
    const api = installTestApi();
    const { adapter } = fixture(({ memory }) => ({
      id: "mutation",
      name: "Mutation",
      memory,
      model: new MastraLanguageModelV2Mock({
        doStream: async () => textStream("done"),
      }),
      instructions: ({ requestContext }) => {
        requestContext.set(key, "other");
        return "Answer";
      },
    }));
    const result = await adapter.stream("hello", {
      memory: { thread: "thread", resource: "resource" },
    });
    await result.consumeStream();
    expect(await result.text).toBe("done");
    await vi.waitFor(() =>
      expect(
        api.calls.find(
          (call) => call.method === "PATCH" && call.body?.status === "failed",
        )?.body?.metadata,
      ).toMatchObject({ mastra_replay_state: "ineligible" }),
    );
  },
);

it("never uploads provider transport credentials in session inputs", async () => {
  const api = installTestApi();
  const { adapter } = fixture();
  const result = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
    providerOptions: {
      openai: { websocket: { headers: { "x-custom-access": "CREDENTIAL" } } },
    },
  });
  await result.consumeStream();
  const session = api.calls.find((call) => call.path === "/api/v1/sessions");
  expect(session?.body?.inputs).toHaveProperty(
    "mastra_memory_replay.complete",
    false,
  );
  expect(JSON.stringify(api.calls)).not.toContain("CREDENTIAL");
});

it("preserves native default-option auth tokens while marking recording ineligible", async () => {
  const api = installTestApi();
  const { adapter } = fixture(({ memory }) => ({
    id: "defaults",
    name: "Defaults",
    memory,
    instructions: "Answer",
    model: new MastraLanguageModelV2Mock({
      doStream: async () => textStream("done"),
    }),
    defaultOptions: ({ requestContext }) => {
      requestContext.setRaw(MASTRA_AUTH_TOKEN_KEY, "CREDENTIAL");
      return {};
    },
  }));
  const result = await adapter.stream("hello", {
    memory: { thread: "thread", resource: "resource" },
  });
  await result.consumeStream();
  expect(await result.text).toBe("done");
  await vi.waitFor(() =>
    expect(
      api.calls.find(
        (call) => call.method === "PATCH" && call.body?.status === "failed",
      )?.body?.metadata,
    ).toMatchObject({ mastra_replay_state: "ineligible" }),
  );
  expect(JSON.stringify(api.calls)).not.toContain("CREDENTIAL");
});

it("marks nested abort signals as unsupported transport configuration", () => {
  const original = input();
  const configuration = { nested: [{ abortSignal: "transport-state" }] };
  expect(
    createMemoryReplayEnvelope({ ...original, configuration }).complete,
  ).toBe(false);
  expect(() =>
    decodeMemoryReplayEnvelope({
      ...createMemoryReplayEnvelope(original),
      configuration,
    }),
  ).toThrow(/transport|sensitive key/i);
});

it("rejects replay envelopes with selectors inconsistent with the snapshot", () => {
  const envelope = createMemoryReplayEnvelope(input());
  expect(() =>
    decodeMemoryReplayEnvelope({
      ...envelope,
      requestContext: { [MASTRA_THREAD_ID_KEY]: "other" },
    }),
  ).toThrow(/selector/i);
  expect(() =>
    decodeMemoryReplayEnvelope({
      ...envelope,
      configuration: {
        runOptions: { memory: { thread: "other", resource: "resource" } },
      },
    }),
  ).toThrow(/selector/i);
});

it("preserves setup errors when settling memory also rejects", async () => {
  const original = new Error("Original setup error");
  const cleanup = new Error("Memory cleanup error");
  const onRecordingError = vi.fn();
  const { adapter, lease } = fixture(
    ({ memory }) => {
      vi.spyOn(memory, "settled").mockRejectedValue(cleanup);
      throw original;
    },
    { onRecordingError },
  );
  await expect(
    adapter.stream("hello", {
      memory: { thread: "thread", resource: "resource" },
    }),
  ).rejects.toBe(original);
  await vi.waitFor(() =>
    expect(onRecordingError).toHaveBeenCalledWith(
      expect.objectContaining({ error: cleanup, stage: "complete" }),
    ),
  );
  const release = await lease.acquire({
    threadId: "thread",
    resourceId: "resource",
  });
  await release();
});

it("preserves native stream errors when cleanup and diagnostics reject", async () => {
  installTestApi();
  const original = new Error("Native stream failed");
  const cleanup = new Error("Cleanup failed");
  const onRecordingError = vi.fn(async () => {
    throw new Error("Diagnostic failed");
  });
  vi.spyOn(Agent.prototype, "stream").mockRejectedValue(original);
  const { adapter } = fixture(
    ({ memory }) => {
      vi.spyOn(memory, "settled").mockRejectedValue(cleanup);
      return {
        id: "failure",
        name: "Failure",
        memory,
        instructions: "Answer",
        model: new MastraLanguageModelV2Mock({}),
      };
    },
    { onRecordingError },
  );
  await expect(
    adapter.stream("hello", {
      memory: { thread: "thread", resource: "resource" },
    }),
  ).rejects.toBe(original);
  await vi.waitFor(() =>
    expect(onRecordingError).toHaveBeenCalledWith(
      expect.objectContaining({ error: cleanup, stage: "complete" }),
    ),
  );
});

it.each([MASTRA_THREAD_ID_KEY, MASTRA_RESOURCE_ID_KEY, MASTRA_AUTH_TOKEN_KEY])(
  "keeps two native streams usable after a processor changes implicit context %s",
  async (key) => {
    const api = installTestApi();
    const modelCall = vi.fn(async () => textStream("done"));
    const processorCall = vi.fn();
    const { adapter } = fixture(({ memory }) => ({
      id: "processor-mutation",
      name: "Processor mutation",
      memory,
      model: new MastraLanguageModelV2Mock({ doStream: modelCall }),
      instructions: "Answer",
      inputProcessors: [
        {
          id: "context-mutation",
          processInput({ requestContext, messages }) {
            processorCall();
            if (!requestContext) throw new Error("Missing request context");
            requestContext.setRaw(key, "FORBIDDEN");
            return messages;
          },
        },
      ],
    }));
    for (let index = 1; index <= 2; index++) {
      const result = await adapter.stream("hello", {
        memory: { thread: "thread", resource: "resource" },
      });
      await result.consumeStream();
      expect(await result.text).toBe("done");
      await vi.waitFor(() =>
        expect(
          api.calls.filter(
            (call) => call.method === "PATCH" && call.body?.status === "failed",
          ),
        ).toHaveLength(index),
      );
    }
    expect(processorCall).toHaveBeenCalledTimes(2);
    expect(modelCall).toHaveBeenCalledTimes(2);
    expect(
      api.calls
        .filter(
          (call) => call.method === "PATCH" && call.body?.status === "failed",
        )
        .every(
          (call) =>
            (call.body?.metadata as Record<string, unknown> | undefined)
              ?.mastra_replay_state === "ineligible",
        ),
    ).toBe(true);
  },
);
