import { Agent } from "@mastra/core/agent";
import type { InputProcessor } from "@mastra/core/processors";
import {
  MASTRA_AUTH_TOKEN_KEY,
  RequestContext,
} from "@mastra/core/request-context";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  settleBuffering,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";

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

import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  type MastraMemoryLease,
  MEMORY_REPLAY_KEY,
} from "../src/memory.js";
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

it("records and replays native evolving memory without re-resolving live configuration", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  let replaying = false;
  const dynamicCalls: string[] = [];
  const requests: unknown[] = [];
  let calls = 0;
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async (args) => {
      requests.push(args);
      return ++calls % 2 === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolCallId: `call-${calls}`,
                toolName: "updateWorkingMemory",
                input: JSON.stringify({
                  memory: {
                    preference: replaying ? "replay-green" : "baseline-red",
                  },
                }),
              },
            ],
            "tool-calls",
          )
        : textStream("done");
    },
  });
  const source = vi.fn(() => ({
    settled: () => runtime.memory.settled(),
    domain: runtime.domain,
    configuration: runtime.memory.getMergedThreadConfig(),
    exclusiveAccess: createProcessLocalMemoryAccess(),
  }));
  const baselineApi = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: { system_prompt: "New instructions" },
      tool_policy: {
        default: { type: "history", scope: "baseline", on_miss: "fail" },
        tools: {},
      },
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "stateful",
      name: "Stateful",
      memory,
      instructions: () => {
        dynamicCalls.push("instructions");
        return "Original instructions";
      },
      model: () => {
        dynamicCalls.push("model");
        return model;
      },
      defaultOptions: () => {
        dynamicCalls.push("defaults");
        return { maxSteps: 3 };
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      sourceMemory: source,
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : model,
    },
  );
  const baseline = await adapter.stream("Green please", {
    memory: { thread: THREAD, resource: RESOURCE },
    context: [
      { role: "system", content: "Extra context. Original instructions" },
    ],
  });
  await baseline.consumeStream();
  expect(dynamicCalls).toEqual(["instructions", "model", "defaults"]);
  await vi.waitFor(() =>
    expect(
      baselineApi.calls.some(
        (call) =>
          call.method === "PATCH" &&
          (call.body?.metadata as Record<string, unknown> | undefined)
            ?.mastra_replay_state === "eligible",
      ),
    ).toBe(true),
  );
  const recorded = baselineApi.calls.find(
    (call) =>
      call.method === "PATCH" &&
      (call.body?.metadata as Record<string, unknown> | undefined)
        ?.mastra_replay_state === "eligible",
  )?.body?.inputs;
  expect(recorded).toHaveProperty(MEMORY_REPLAY_KEY);
  expect(
    baselineApi.calls.find(
      (call) => call.method === "POST" && call.path === "/api/v1/sessions",
    )?.body?.metadata,
  ).toMatchObject({
    mastra_replay_state: "pending",
    mastra_native_state: "pending",
  });
  expect(
    baselineApi.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
  ).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "eligible",
      mastra_native_state: "completed",
    },
  });
  const baselineNodes = baselineApi.nodeBatches().flat();
  expect(baselineNodes.some((node) => node.name === "memory_mutation")).toBe(
    true,
  );
  expect(
    baselineNodes
      .filter((node) => node.node_type === "llm_call")
      .every((node) => node.inputs),
  ).toBe(true);
  expect(
    baselineNodes.find((node) => node.node_type === "llm_call")?.attributes,
  ).toHaveProperty(
    "prompt_provenance.extraContextRef",
    "mastra_memory_replay.configuration",
  );
  await runtime.memory.updateWorkingMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    workingMemory: JSON.stringify({ preference: "production-today" }),
  });
  replaying = true;
  source.mockImplementation(() => {
    throw new Error("Production source used during replay");
  });
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(recorded));
  const replayApi = baselineApi;
  const replay = await adapter.stream("ignored", {
    memory: { thread: "today", resource: "today" },
  });
  await replay.consumeStream();
  expect(dynamicCalls).toEqual(["instructions", "model", "defaults"]);
  expect(source).toHaveBeenCalledTimes(1);
  expect(JSON.stringify(requests[2])).toContain("historical-blue");
  expect(JSON.stringify(requests[2])).toContain("New instructions");
  expect(
    JSON.stringify(requests[2]).match(/Original instructions/g),
  ).toHaveLength(1);
  expect(JSON.stringify(requests[2])).toContain("Extra context");
  expect(JSON.stringify(requests[3])).toContain("replay-green");
  expect(
    replayApi.calls.filter((call) => call.method === "PATCH").at(-1)?.body
      ?.status,
  ).toBe("completed");
  expect(
    replayApi.calls.some((call) => call.path.endsWith("tool-lookup")),
  ).toBe(false);
  expect(
    await runtime.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("production-today");
  await runtime.store.close();
});

it("finishes the native stream before delayed observation and finalizes replay later", async () => {
  let release!: () => void;
  let started!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const observing = new Promise<void>((resolve) => {
    started = resolve;
  });
  const runtime = createMemoryRuntime({
    messageTokens: 10000,
    observerWait: async () => {
      started();
      await blocked;
    },
  });
  await seedMemory(runtime);
  const api = installTestApi();
  const lease = createProcessLocalMemoryAccess();
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("done"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "settled",
      name: "Settled",
      instructions: "Answer",
      model,
      memory,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: lease,
      }),
      resolveModel: () => model,
    },
  );
  try {
    const result = await adapter.stream("Observe this message", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    const consuming = result.consumeStream();
    await observing;
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(
      api.calls.some(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toBe(false);
    await consuming;
    expect(await result.text).toBe("done");
    expect(
      api.calls.some(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toBe(false);
    release();
    await vi.waitFor(() =>
      expect(
        api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
      ).toMatchObject({
        status: "completed",
        metadata: { mastra_replay_state: "eligible" },
      }),
    );
    const releaseLease = await lease.acquire({
      threadId: THREAD,
      resourceId: RESOURCE,
    });
    await releaseLease();
    expect(runtime.observer.calls.length).toBeGreaterThan(0);
    const observerCalls = runtime.observer.calls.length;
    const reflectorCalls = runtime.reflector.calls.length;
    const finalizedInputs = api.calls.find(
      (call) =>
        call.method === "PATCH" &&
        (call.body?.metadata as Record<string, unknown> | undefined)
          ?.mastra_replay_state === "eligible",
    )?.body?.inputs;
    expect(finalizedInputs).toHaveProperty(MEMORY_REPLAY_KEY);
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(finalizedInputs));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    expect(await replay.text).toBe("done");
    expect(runtime.observer.calls).toHaveLength(observerCalls);
    expect(runtime.reflector.calls).toHaveLength(reflectorCalls);
    expect(
      api
        .nodeBatches()
        .flat()
        .some(
          (node) =>
            node.name === "memory_mutation" &&
            (node.attributes as Record<string, unknown>).memory_method ===
              "updateBufferedObservations",
        ),
    ).toBe(true);
  } finally {
    release();
    await runtime.memory.settled();
    await runtime.store.close();
  }
});

it("keeps baseline output when initial snapshot evidence fails and rejects its incomplete replay", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  vi.spyOn(runtime.domain, "getResourceById").mockRejectedValueOnce(
    new Error("Snapshot read failed"),
  );
  const source = vi.fn(() => ({
    settled: () => runtime.memory.settled(),
    domain: runtime.domain,
    configuration: runtime.memory.getMergedThreadConfig(),
    exclusiveAccess: createProcessLocalMemoryAccess(),
  }));
  const modelCalls = vi.fn(async () => textStream("native output"));
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: modelCalls,
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "partial",
      name: "Partial",
      instructions: "Answer",
      memory,
      model,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: source,
      resolveModel: () => model,
    },
  );
  const output = await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  expect(await output.text).toBe("native output");
  const input = api.calls.find(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  )?.body?.inputs as Record<string, { complete: boolean; reasons: string[] }>;
  expect(input[MEMORY_REPLAY_KEY]?.complete).toBe(false);
  // The envelope names the failed read without copying the storage error.
  expect(input[MEMORY_REPLAY_KEY]?.reasons.join(" ")).toMatch(
    /could not be read from storage/,
  );
  expect(JSON.stringify(api.calls)).not.toContain("Snapshot read failed");
  await vi.waitFor(() =>
    expect(
      api.calls.find(
        (call) =>
          call.method === "PATCH" &&
          call.body?.status === "completed" &&
          call.body?.metadata &&
          (call.body.metadata as Record<string, unknown>)
            .mastra_replay_state === "ineligible",
      )?.body,
    ).toMatchObject({
      metadata: {
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "memory_read_failed",
        mastra_native_state: "completed",
      },
      outputs: { text: "native output" },
    }),
  );
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
  await expect(adapter.stream("ignored")).rejects.toThrow(
    /complete version-3/i,
  );
  expect(source).toHaveBeenCalledTimes(1);
  expect(modelCalls).toHaveBeenCalledTimes(1);
  await runtime.store.close();
});

it("runs natively when request context cannot be captured safely", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  const access = createProcessLocalMemoryAccess();
  const unsafeWrite = vi.spyOn(access, "markUnsafeWrite");
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "unsupported-context",
      name: "Unsupported context",
      instructions: "Answer",
      memory,
      model,
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
      resolveModel: () => model,
      captureRequestContext: () => ({ [MASTRA_AUTH_TOKEN_KEY]: "secret" }),
    },
  );
  const output = await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  expect(await output.text).toBe("native answer");
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
  expect(JSON.stringify(api.calls)).not.toContain("secret");
  // The fallback registered each write, so nothing outlives its native turn.
  expect(unsafeWrite).not.toHaveBeenCalled();
  const next = await access.acquire({ threadId: THREAD, resourceId: RESOURCE });
  expect(await next.verifyEligibility()).toBe(true);
  await next();
  await runtime.memory.settled();
  await runtime.store.close();
});

it("registers native fallback writes under an implicit default selector", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  installTestApi();
  const access = createProcessLocalMemoryAccess();
  const unsafeWrite = vi.spyOn(access, "markUnsafeWrite");
  const acquire = vi.spyOn(access, "acquire");
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "implicit-native-fallback",
      name: "Implicit native fallback",
      instructions: "Answer",
      memory,
      model,
      defaultOptions: {
        memory: { thread: THREAD, resource: RESOURCE },
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
      resolveModel: () => model,
    },
  );
  try {
    const output = await adapter.stream("Hello");
    await output.consumeStream();
    expect(await output.text).toBe("native answer");
    expect(unsafeWrite).not.toHaveBeenCalled();
    expect(acquire).toHaveBeenCalledWith(
      { threadId: THREAD, resourceId: RESOURCE },
      expect.objectContaining({ waitMs: 0 }),
    );
    const next = await access.acquire({
      threadId: THREAD,
      resourceId: RESOURCE,
    });
    expect(await next.verifyEligibility()).toBe(true);
    await next();
  } finally {
    await runtime.store.close();
  }
});

it("keeps the native answer moving when unsafe-write coordination hangs", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  installTestApi();
  const reported = vi.fn();
  const access = {
    ...createProcessLocalMemoryAccess(),
    acquire: vi.fn(() => new Promise<MastraMemoryLease>(() => undefined)),
    markUnsafeWrite: vi.fn(() => new Promise<void>(() => undefined)),
  };
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "hung-unsafe-marker",
      name: "Hung unsafe marker",
      instructions: "Answer",
      memory,
      model,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      onRecordingError: reported,
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: access,
      }),
      resolveModel: () => model,
      captureRequestContext: () => ({ [MASTRA_AUTH_TOKEN_KEY]: "secret" }),
    },
  );
  try {
    const output = await adapter.stream("Hello", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    expect(await output.text).toBe("native answer");
    // One persistent marker covers the fallback, so later writes skip waiting.
    expect(access.markUnsafeWrite).toHaveBeenCalledOnce();
    await vi.waitFor(() =>
      expect(reported).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.objectContaining({
            message: "Unsafe memory write could not be fenced.",
          }),
        }),
      ),
    );
  } finally {
    await runtime.store.close();
  }
}, 2000);

it("classifies an unrelated resource setup error as capture setup failure", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  let factoryCalls = 0;
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => {
      if (++factoryCalls === 1)
        throw new Error("resource registry temporarily unavailable");
      return {
        id: "resource-setup-failure",
        name: "Resource setup failure",
        instructions: "Answer",
        model,
        memory,
      };
    },
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
    },
  );
  try {
    const output = await adapter.stream("Hello", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    expect(await output.text).toBe("native answer");
    await vi.waitFor(() =>
      expect(
        api.calls.find(
          (call) => call.method === "POST" && call.path === "/api/v1/sessions",
        )?.body?.metadata,
      ).toMatchObject({ mastra_replay_reason: "capture_setup_failed" }),
    );
  } finally {
    await runtime.store.close();
  }
});

it("passes the original request context to baseline resolvers while recording only approved values", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  const seen: unknown[] = [];
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "native-context",
      name: "Native context",
      instructions: ({ requestContext }) => {
        seen.push(requestContext.get("accessToken"));
        return "Answer";
      },
      memory,
      model,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
      captureRequestContext: (context) => ({ locale: context.get("locale") }),
    },
  );
  const context = new RequestContext();
  context.set("accessToken", "private-token");
  context.set("locale", "nl");
  const output = await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
    requestContext: context,
  });
  await output.consumeStream();
  expect(await output.text).toBe("native answer");
  expect(seen).toContain("private-token");
  expect(JSON.stringify(api.calls)).not.toContain("private-token");
  const recordedInputs = api.calls.find(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  )?.body?.inputs as Record<string, { requestContext: unknown }> | undefined;
  expect(recordedInputs?.[MEMORY_REPLAY_KEY]?.requestContext).toMatchObject({
    locale: "nl",
  });
  await runtime.memory.settled();
  await runtime.store.close();
});

it("runs natively and reports locally when Kitaru session creation fails", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  installTestApi();
  const reported = vi.fn();
  const originalFetch = globalThis.fetch;
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof globalThis.fetch>(async (input, init) => {
      if (
        init?.method === "POST" &&
        new URL(String(input)).pathname === "/api/v1/sessions"
      )
        throw new Error("Kitaru unavailable");
      return originalFetch(input, init);
    }),
  );
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("native answer"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "outage",
      name: "Outage",
      instructions: "Answer",
      memory,
      model,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      onRecordingError: reported,
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
    },
  );
  const output = await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  expect(await output.text).toBe("native answer");
  await vi.waitFor(() => expect(reported).toHaveBeenCalledTimes(1));
  expect(reported.mock.calls[0]?.[0]).toMatchObject({
    reason: "recording_setup_failed",
    stage: "setup",
  });
  await runtime.memory.settled();
  await runtime.store.close();
});

it("reports OM divergence when an override adds a new memory call", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 600 });
  await seedMemory(runtime);
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: { model: "fixture/replacement" },
      tool_policy: { default: { type: "passthrough" }, tools: {} },
    },
  });
  const requests: unknown[] = [];
  let calls = 0;
  const replacement = new MastraLanguageModelV2Mock({
    modelId: "replacement",
    provider: "fixture",
    doStream: async (args) => {
      requests.push(args);
      calls++;
      return calls === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolCallId: "change-memory",
                toolName: "updateWorkingMemory",
                input: JSON.stringify({
                  memory: { preference: "replay-green" },
                }),
              },
            ],
            "tool-calls",
          )
        : calls === 2
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolCallId: "evidence",
                  toolName: "readEvidence",
                  input: "{}",
                },
              ],
              "tool-calls",
            )
          : textStream("evolved");
    },
  });
  const original = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("original"),
  });
  const { createTool } = await import("@mastra/core/tools");
  const { z } = await import("zod/v4");
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "evolving",
      name: "Evolving",
      instructions: "Use evidence",
      model: original,
      memory,
      defaultOptions: { maxSteps: 5 },
      tools: {
        readEvidence: createTool({
          id: "readEvidence",
          description: "Read conversation evidence",
          inputSchema: z.object({}),
          execute: async () => {
            await memory.settled();
            return {
              evidence: "The replay user now prefers green. ".repeat(400),
            };
          },
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      allowedReplayModels: ["fixture/replacement"],
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : id === "fixture/replacement"
              ? replacement
              : original,
    },
  );
  const baseline = await adapter.stream("Baseline turn", {
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
  const observerCalls = runtime.observer.calls.length;
  const reflectorCalls = runtime.reflector.calls.length;
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
  const nativeStream = Agent.prototype.stream;
  let nativeResult: unknown;
  vi.spyOn(Agent.prototype, "stream").mockImplementation(async function (
    this: Agent,
    ...args
  ) {
    nativeResult = await Reflect.apply(nativeStream, this, args);
    return nativeResult as Awaited<ReturnType<Agent["stream"]>>;
  });
  const output = await adapter.stream("ignored");
  expect(output).toBe(nativeResult);
  await output.consumeStream();
  expect(await output.text).toBe("evolved");
  expect(runtime.observer.calls).toHaveLength(observerCalls);
  expect(runtime.reflector.calls).toHaveLength(reflectorCalls);
  await vi.waitFor(() =>
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
    ).toMatchObject({
      status: "failed",
      error: "KITARU_REPLAY_DIVERGED:mastra_om_call_order",
      metadata: {
        mastra_replay_state: "diverged",
        mastra_replay_reason: "mastra_om_call_order",
      },
    }),
  );
  expect(
    await runtime.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("historical-blue");
  await runtime.store.close();
});

it("releases the source lease on setup failure and cancellation", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  const lease = createProcessLocalMemoryAccess();
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("unused"),
  });
  let failSetup = true;
  const adapter = createMemoryReplayAgent(
    ({ memory }) => {
      if (failSetup) throw new Error("Factory failed");
      return {
        id: "cancelled",
        name: "Cancelled",
        instructions: "Answer",
        model,
        memory,
      };
    },
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: lease,
      }),
      resolveModel: () => model,
    },
  );
  await expect(
    adapter.stream("Hello", { memory: { thread: THREAD, resource: RESOURCE } }),
  ).rejects.toThrow("Factory failed");
  const firstRelease = await lease.acquire({
    threadId: THREAD,
    resourceId: RESOURCE,
  });
  await firstRelease();
  failSetup = false;
  const abort = new AbortController();
  abort.abort(new Error("Cancelled"));
  const output = await adapter.stream("Hello", {
    memory: { thread: THREAD, resource: RESOURCE },
    abortSignal: abort.signal,
  });
  await output.consumeStream();
  await vi.waitFor(() =>
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body?.status,
    ).toBe("failed"),
  );
  const release = await lease.acquire({
    threadId: THREAD,
    resourceId: RESOURCE,
  });
  await release();
  await runtime.store.close();
});

it("preserves native continuation after a memory storage write fails", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const api = installTestApi();
  const lease = createProcessLocalMemoryAccess();
  const { createTool } = await import("@mastra/core/tools");
  const { z } = await import("zod/v4");
  const external = vi.fn(async () => "side effect");
  let actorCalls = 0;
  const patchThread = runtime.domain.patchThread.bind(runtime.domain);
  vi.spyOn(runtime.domain, "patchThread").mockImplementation(
    async (...args) => {
      if (actorCalls > 0) throw new Error("Native memory write failed");
      return patchThread(...args);
    },
  );
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () =>
      ++actorCalls === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolName: "updateWorkingMemory",
                toolCallId: "memory-failure",
                input: JSON.stringify({ memory: { preference: "green" } }),
              },
              {
                type: "tool-call",
                toolName: "external",
                toolCallId: "later-tool",
                input: "{}",
              },
            ],
            "tool-calls",
          )
        : textStream("native continuation"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "write-failure",
      name: "Write failure",
      instructions: "Update memory",
      model,
      memory,
      defaultOptions: { maxSteps: 3, toolCallConcurrency: 1 },
      tools: {
        external: createTool({
          id: "external",
          description: "A side effect",
          inputSchema: z.object({}),
          execute: external,
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
        exclusiveAccess: lease,
      }),
      resolveModel: () => model,
    },
  );
  try {
    const onFinish = vi.fn();
    const output = await adapter.stream("Remember green", {
      memory: { thread: THREAD, resource: RESOURCE },
      onFinish,
    });
    await output.consumeStream();
    expect(await output.text).toBe("native continuation");
    expect(onFinish).toHaveBeenCalledTimes(1);
    expect(onFinish.mock.calls[0]?.[0]?.steps?.at(-1)?.finishReason).toBe(
      "stop",
    );
    await vi.waitFor(() =>
      expect(
        api.calls.filter((call) => call.method === "PATCH").at(-1)?.body
          ?.status,
      ).toBe("completed"),
    );
    expect(actorCalls).toBe(2);
    expect(external).toHaveBeenCalledTimes(1);
    expect(
      api.calls.find(
        (call) =>
          call.method === "PATCH" &&
          call.body?.status === "completed" &&
          (call.body?.metadata as Record<string, unknown> | undefined)
            ?.mastra_replay_state === "ineligible",
      )?.body?.metadata,
    ).toMatchObject({
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "memory_mutation_failed",
      mastra_native_state: "completed",
    });
    const release = await lease.acquire({
      threadId: THREAD,
      resourceId: RESOURCE,
    });
    await release();
  } finally {
    await runtime.memory.settled();
    await runtime.store.close();
  }
});
