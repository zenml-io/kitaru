import { Agent } from "@mastra/core/agent";
import { MastraMemory } from "@mastra/core/memory";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import type { RuntimeStreamOptions } from "../src/types.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  invokeTool,
  textStep,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

class StreamHistoryMemory extends MastraMemory {
  readonly domain;

  constructor() {
    const storage = new InMemoryStore();
    super({ name: "stream-history", options: { lastMessages: 10 }, storage });
    const domain = storage.stores.memory;
    if (!domain) throw new Error("Missing in-memory message storage");
    this.domain = domain;
  }

  getThreadById = (args: Parameters<MastraMemory["getThreadById"]>[0]) =>
    this.domain.getThreadById(args);
  listThreads = (args: Parameters<MastraMemory["listThreads"]>[0]) =>
    this.domain.listThreads(args);
  saveThread = (args: Parameters<MastraMemory["saveThread"]>[0]) =>
    this.domain.saveThread(args);
  saveMessages = (args: Parameters<MastraMemory["saveMessages"]>[0]) =>
    this.domain.saveMessages(args);
  recall = (args: Parameters<MastraMemory["recall"]>[0]) =>
    this.domain.listMessages(args);
  updateThread = (args: Parameters<MastraMemory["updateThread"]>[0]) =>
    this.domain.updateThread(args);
  deleteThread = (threadId: string) => this.domain.deleteThread({ threadId });
  cloneThread = (args: Parameters<MastraMemory["cloneThread"]>[0]) =>
    this.domain.cloneThread(args);
  async deleteMessages(): Promise<never> {
    throw new Error("Not used by stream history fixture");
  }
  async getWorkingMemory(): Promise<null> {
    return null;
  }
  async getWorkingMemoryTemplate(): Promise<null> {
    return null;
  }
  async updateWorkingMemory(): Promise<never> {
    throw new Error("Not used by stream history fixture");
  }
  async __experimental_updateWorkingMemoryVNext(): Promise<never> {
    throw new Error("Not used by stream history fixture");
  }
}

function chunks(text: string): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: "stream-start", warnings: [] });
      controller.enqueue({
        id: "response-lifecycle",
        modelId: "served-lifecycle",
        type: "response-metadata",
      });
      controller.enqueue({ id: "text", type: "text-start" });
      controller.enqueue({ delta: text, id: "text", type: "text-delta" });
      controller.enqueue({ id: "text", type: "text-end" });
      controller.enqueue({
        finishReason: "stop",
        type: "finish",
        usage: { inputTokens: 2, outputTokens: 2, totalTokens: 4 },
      });
      controller.close();
    },
  });
}

function agentFor(text = "done"): Agent {
  return new Agent({
    id: `lifecycle-${text.length}`,
    instructions: "Respond.",
    model: new MastraLanguageModelV2Mock({
      doStream: async () => ({ stream: chunks(text) as never }),
      modelId: "lifecycle-model",
      provider: "test-provider",
    }),
    name: "Lifecycle",
  });
}

function failingAgent(
  failure = new Error("native model stream failed"),
): Agent {
  return new Agent({
    id: "failing-stream",
    instructions: "Respond.",
    model: new MastraLanguageModelV2Mock({
      doStream: async () => {
        let pulled = false;
        return {
          stream: new ReadableStream({
            async pull(controller) {
              if (!pulled) {
                pulled = true;
                controller.enqueue({ id: "error", type: "text-start" });
                controller.enqueue({
                  delta: "partial",
                  id: "error",
                  type: "text-delta",
                });
                return;
              }
              controller.error(failure);
            },
          }) as never,
        };
      },
      modelId: "failing-model",
      provider: "test-provider",
    }),
    name: "Failing stream",
  });
}

function abortableAgent(): Agent {
  return new Agent({
    id: "abortable-stream",
    instructions: "Respond.",
    model: new MastraLanguageModelV2Mock({
      doStream: async (options) => {
        let pulled = false;
        return {
          stream: new ReadableStream({
            async pull(controller) {
              if (!pulled) {
                pulled = true;
                controller.enqueue({ id: "abort", type: "text-start" });
                controller.enqueue({
                  delta: "partial",
                  id: "abort",
                  type: "text-delta",
                });
                return;
              }
              if (!options.abortSignal?.aborted) {
                await new Promise<void>((resolve) =>
                  options.abortSignal?.addEventListener(
                    "abort",
                    () => resolve(),
                    {
                      once: true,
                    },
                  ),
                );
              }
              controller.error(
                options.abortSignal?.reason ?? new Error("aborted"),
              );
            },
          }) as never,
        };
      },
      modelId: "abortable-model",
      provider: "test-provider",
    }),
    name: "Abortable stream",
  });
}

function cancellableAgent(): Agent {
  return new Agent({
    id: "cancellable-stream",
    instructions: "Respond.",
    model: new MastraLanguageModelV2Mock({
      doStream: async () => {
        const values = [
          { type: "stream-start", warnings: [] },
          { id: "cancel", type: "text-start" },
          { delta: "partial", id: "cancel", type: "text-delta" },
          { id: "cancel", type: "text-end" },
          {
            finishReason: "stop",
            type: "finish",
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          },
        ];
        let index = 0;
        return {
          stream: new ReadableStream({
            async pull(controller) {
              if (index >= 3) {
                await new Promise((resolve) => setTimeout(resolve, 100));
              }
              const value = values[index++];
              if (value) controller.enqueue(value);
              else controller.close();
            },
          }) as never,
        };
      },
      modelId: "cancellable-model",
      provider: "test-provider",
    }),
    name: "Cancellable stream",
  });
}

async function text(output: {
  textStream: {
    getReader(): {
      read(): Promise<{ done: boolean; value?: string }>;
    };
  };
}): Promise<string> {
  const reader = output.textStream.getReader();
  let result = "";
  while (true) {
    const next = await reader.read();
    if (next.done) return result;
    result += next.value ?? "";
  }
}

async function errorChannels(
  output: Parameters<typeof text>[0] & {
    getFullOutput(): Promise<unknown>;
  },
  first: "aggregate" | "text",
): Promise<{ aggregate: unknown; text: "rejected" | "resolved" | "timeout" }> {
  const readAggregate = () => output.getFullOutput().catch((error) => error);
  const readText = () =>
    Promise.race([
      text(output).then(
        () => "resolved" as const,
        () => "rejected" as const,
      ),
      new Promise<"timeout">((resolve) =>
        setTimeout(() => resolve("timeout"), 50),
      ),
    ]);
  if (first === "aggregate") {
    const aggregate = await readAggregate();
    return { aggregate, text: await readText() };
  }
  const textResult = await readText();
  return { aggregate: await readAggregate(), text: textResult };
}

function wrapFetch(
  reject: (call: {
    body?: Record<string, unknown>;
    method: string;
    path: string;
  }) => boolean,
): void {
  const original = globalThis.fetch;
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof globalThis.fetch>(async (input, init) => {
      const url = new URL(String(input));
      const body = init?.body
        ? (JSON.parse(String(init.body)) as Record<string, unknown>)
        : undefined;
      if (reject({ body, method: init?.method ?? "GET", path: url.pathname })) {
        return new Response(JSON.stringify({ detail: "capture failed" }), {
          headers: { "Content-Type": "application/json" },
          status: 500,
        });
      }
      return original(input, init);
    }),
  );
}

function enforceTerminalSessionTransitions(): { attempts: string[] } {
  const original = globalThis.fetch;
  const attempts: string[] = [];
  const terminalSessions = new Set<string>();
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof globalThis.fetch>(async (input, init) => {
      const url = new URL(String(input));
      const body = init?.body
        ? (JSON.parse(String(init.body)) as Record<string, unknown>)
        : undefined;
      const sessionId = url.pathname.match(
        /^\/api\/v1\/sessions\/([^/]+)/,
      )?.[1];
      if (
        init?.method === "POST" &&
        url.pathname.endsWith("/nodes") &&
        sessionId &&
        terminalSessions.has(sessionId)
      ) {
        return new Response(
          JSON.stringify({ detail: "session is already terminal" }),
          {
            headers: { "Content-Type": "application/json" },
            status: 409,
          },
        );
      }
      const status = body?.status;
      if (
        init?.method === "PATCH" &&
        url.pathname.startsWith("/api/v1/sessions/") &&
        (status === "completed" || status === "failed")
      ) {
        attempts.push(status);
        if (sessionId && terminalSessions.has(sessionId)) {
          return new Response(
            JSON.stringify({ detail: "session is already terminal" }),
            {
              headers: { "Content-Type": "application/json" },
              status: 409,
            },
          );
        }
        const response = await original(input, init);
        if (response.ok && sessionId) terminalSessions.add(sessionId);
        return response;
      }
      return original(input, init);
    }),
  );
  return { attempts };
}

describe("stream recording lifecycle", () => {
  it.each([
    {
      expectedModelError: "blocked by policy",
      expectedRunError: "blocked by policy",
      reason: "blocked by policy",
    },
    {
      expectedModelError: "Model step failed",
      expectedRunError: "Mastra processor tripwire triggered",
      reason: "",
    },
  ])(
    "records a tripwire with reason '$reason' as failed without changing the native result",
    async ({ expectedModelError, expectedRunError, reason }) => {
      const api = installTestApi();
      const tripwire = {
        metadata: { category: "policy" },
        processorId: "guard",
        reason,
      };
      const step = {
        ...textStep(`tripwire-${reason.length}`),
        finishReason: "tripwire",
        tripwire,
      };
      const nativeResult = { text: "", tripwire };
      const agent = Object.assign(new FakeAgent(), {
        async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
          await options.onStepFinish?.(step as never);
          await options.onFinish?.({
            ...step,
            steps: [step],
            text: "",
            totalUsage: step.usage,
          } as never);
          return nativeResult;
        },
      });
      const recorded = new KitaruAgent(agent, {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "tripwire-model",
      });

      await expect(recorded.stream("hello")).resolves.toBe(nativeResult);

      const nodes = api.nodeBatches().flat();
      expect(nodes.find((node) => node.node_type === "llm_call")).toMatchObject(
        {
          error: expectedModelError,
          outputs: { tripwire },
          status: "failed",
        },
      );
      expect(
        nodes.find((node) => node.name === "run" && node.status === "failed"),
      ).toMatchObject({ error: expectedRunError, status: "failed" });
      expect(api.calls.at(-1)?.body).toMatchObject({
        error: expectedRunError,
        status: "failed",
      });
    },
  );

  it("does not rewrite completion when abort arrives after the terminal decision", async () => {
    const api = installTestApi();
    const terminal = enforceTerminalSessionTransitions();
    const original = globalThis.fetch;
    let releasePatch: (() => void) | undefined;
    let patchStarted: (() => void) | undefined;
    const patchGate = new Promise<void>((resolve) => {
      releasePatch = resolve;
    });
    const started = new Promise<void>((resolve) => {
      patchStarted = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const body = init?.body
          ? (JSON.parse(String(init.body)) as Record<string, unknown>)
          : undefined;
        if (init?.method === "PATCH" && body?.status === "completed") {
          patchStarted?.();
          await patchGate;
        }
        return original(input, init);
      }),
    );
    let runtimeOptions: RuntimeStreamOptions | undefined;
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        runtimeOptions = options;
        return { native: true };
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });
    await recorded.stream("hello");

    const finish = runtimeOptions?.onFinish?.({
      ...textStep("terminal-race"),
      steps: [],
      text: "done",
      totalUsage: textStep("terminal-race").usage,
    });
    await started;
    const abort = runtimeOptions?.onAbort?.({ steps: [], text: "done" });
    releasePatch?.();
    await Promise.all([finish, abort]);

    const statuses = api.calls
      .filter((call) => call.method === "PATCH")
      .map((call) => call.body?.status);
    expect(terminal.attempts).toEqual(["completed"]);
    expect(statuses).toEqual(["completed"]);
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "completed" });
  });

  it("waits for a whole pending step write before publishing completion", async () => {
    const api = installTestApi();
    const original = globalThis.fetch;
    let releaseWrite: (() => void) | undefined;
    const writeGate = new Promise<void>((resolve) => {
      releaseWrite = resolve;
    });
    let nodeWrites = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (init?.method === "POST" && url.pathname.endsWith("/nodes")) {
          nodeWrites += 1;
          if (nodeWrites === 2) await writeGate;
        }
        return original(input, init);
      }),
    );
    let pending: Promise<unknown>[] = [];
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        pending = [
          Promise.resolve(options.onStepFinish?.(textStep("overlap"))),
          Promise.resolve(
            options.onFinish?.({
              ...textStep("overlap"),
              steps: [textStep("overlap")],
              text: "overlap",
              totalUsage: textStep("overlap").usage,
            }),
          ),
        ];
        return { native: true };
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });

    await recorded.stream("hello");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(
      api.calls.some(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toBe(false);

    releaseWrite?.();
    await Promise.all(pending);
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "completed" });
  });

  it("serializes a concurrent step-write failure, finish, and abort as one failed terminal", async () => {
    const api = installTestApi();
    const original = globalThis.fetch;
    let releaseWrite: (() => void) | undefined;
    let writeStarted: (() => void) | undefined;
    const writeGate = new Promise<void>((resolve) => {
      releaseWrite = resolve;
    });
    const started = new Promise<void>((resolve) => {
      writeStarted = resolve;
    });
    let nodeWrites = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (init?.method === "POST" && url.pathname.endsWith("/nodes")) {
          nodeWrites += 1;
          if (nodeWrites === 2) {
            writeStarted?.();
            await writeGate;
            return new Response(JSON.stringify({ detail: "step failed" }), {
              headers: { "Content-Type": "application/json" },
              status: 500,
            });
          }
        }
        return original(input, init);
      }),
    );
    let runtimeOptions: RuntimeStreamOptions | undefined;
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        runtimeOptions = options;
        return { native: true };
      },
    });
    const reported = vi.fn();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onRecordingError: reported,
      requestedModelId: "lifecycle-model",
    });
    await recorded.stream("hello");

    const step = runtimeOptions?.onStepFinish?.(textStep("race"));
    await started;
    const finish = runtimeOptions?.onFinish?.({
      ...textStep("race"),
      steps: [textStep("race")],
      text: "done",
      totalUsage: textStep("race").usage,
    });
    const abort = runtimeOptions?.onAbort?.({ steps: [], text: "partial" });
    releaseWrite?.();
    await Promise.all([step, finish, abort]);

    await vi.waitFor(() => expect(reported).toHaveBeenCalledTimes(1));
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
    const terminalStatuses = api.calls
      .filter((call) => call.method === "PATCH")
      .map((call) => call.body?.status);
    expect(terminalStatuses).toEqual(["failed"]);
  });

  it("lets abort win before the terminal decision and retains queued steps", async () => {
    const api = installTestApi();
    const terminal = enforceTerminalSessionTransitions();
    const original = globalThis.fetch;
    let releaseFirst: (() => void) | undefined;
    let firstStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const started = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });
    let nodeWrites = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (init?.method === "POST" && url.pathname.endsWith("/nodes")) {
          nodeWrites += 1;
          if (nodeWrites === 2) {
            firstStarted?.();
            await firstGate;
          }
        }
        return original(input, init);
      }),
    );
    let runtimeOptions: RuntimeStreamOptions | undefined;
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        runtimeOptions = options;
        return { native: true };
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });
    await recorded.stream("hello");

    const first = runtimeOptions?.onStepFinish?.(textStep("queued-one"));
    await started;
    const second = runtimeOptions?.onStepFinish?.(textStep("queued-two"));
    await Promise.resolve();
    const finish = runtimeOptions?.onFinish?.({
      ...textStep("queued-two"),
      steps: [textStep("queued-one"), textStep("queued-two")],
      text: "done",
      totalUsage: textStep("queued-two").usage,
    });
    const abort = runtimeOptions?.onAbort?.({ steps: [], text: "partial" });
    releaseFirst?.();
    await Promise.all([first, second, finish, abort]);

    const llmNodes = api
      .nodeBatches()
      .flat()
      .filter((node) => node.node_type === "llm_call");
    expect(llmNodes.map((node) => node.external_id)).toEqual([
      "response-queued-one",
      "response-queued-two",
    ]);
    expect(terminal.attempts).toEqual(["failed"]);
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
  });

  it.each(["step", "root", "patch"])(
    "contains a %s recording failure and reports it once",
    async (failure) => {
      const api = installTestApi();
      let nodeWrites = 0;
      wrapFetch(({ body, method, path }) => {
        if (method === "POST" && path.endsWith("/nodes")) {
          nodeWrites += 1;
          if (failure === "step" && nodeWrites === 2) return true;
          if (
            failure === "root" &&
            Array.isArray(body?.nodes) &&
            body.nodes.some(
              (node) =>
                typeof node === "object" &&
                node !== null &&
                (node as Record<string, unknown>).external_id === "run" &&
                (node as Record<string, unknown>).status === "completed",
            )
          ) {
            return true;
          }
        }
        return (
          failure === "patch" &&
          method === "PATCH" &&
          body?.status === "completed"
        );
      });
      const reported = vi.fn();
      const recorded = new KitaruAgent(agentFor(), {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        onRecordingError: reported,
        requestedModelId: "lifecycle-model",
      });

      const output = await recorded.stream("hello");
      await expect(text(output)).resolves.toBe("done");
      await expect(output.getFullOutput()).resolves.toMatchObject({
        text: "done",
      });
      await vi.waitFor(() => expect(reported).toHaveBeenCalledTimes(1));

      expect(reported.mock.calls[0]?.[0]).toMatchObject({
        error: expect.anything(),
        sessionId: api.sessionIds[0],
        stage: failure === "step" ? "step" : "complete",
      });
      expect(
        api.calls.some(
          (call) => call.method === "PATCH" && call.body?.status === "failed",
        ),
      ).toBe(true);
      expect(
        api.calls.some(
          (call) =>
            call.method === "PATCH" && call.body?.status === "completed",
        ),
      ).toBe(false);
    },
  );

  it("does not let a step recording failure disable later application tools", async () => {
    installTestApi();
    let nodeWrites = 0;
    wrapFetch(({ method, path }) => {
      if (method === "POST" && path.endsWith("/nodes")) {
        nodeWrites += 1;
        return nodeWrites === 2;
      }
      return false;
    });
    const execute = vi.fn(() => ({ queued: true }));
    const reported = vi.fn();
    const nativeResult = { text: "refund queued" };
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        await options.onStepFinish?.(textStep("before-tool"));
        const output = await invokeTool(options.hooks ?? {}, {
          args: { orderId: "order-1" },
          callId: "refund-1",
          execute,
          output: undefined,
          toolName: "queueRefundReview",
        });
        expect(output).toEqual({ queued: true });
        return nativeResult;
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onRecordingError: reported,
      requestedModelId: "lifecycle-model",
    });

    await expect(recorded.stream("hello")).resolves.toBe(nativeResult);
    expect(execute).toHaveBeenCalledTimes(1);
    await vi.waitFor(() => expect(reported).toHaveBeenCalledTimes(1));
  });

  it("reports a step write rejected after terminal completion", async () => {
    installTestApi();
    enforceTerminalSessionTransitions();
    let runtimeOptions: RuntimeStreamOptions | undefined;
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        runtimeOptions = options;
        return { native: true };
      },
    });
    const reported = vi.fn();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onRecordingError: reported,
      requestedModelId: "lifecycle-model",
    });
    await recorded.stream("hello");
    await runtimeOptions?.onFinish?.({
      ...textStep("complete-first"),
      steps: [],
      text: "done",
      totalUsage: textStep("complete-first").usage,
    });

    await runtimeOptions?.onStepFinish?.(textStep("too-late"));

    await vi.waitFor(() => expect(reported).toHaveBeenCalledTimes(1));
    expect(reported.mock.calls[0]?.[0]).toMatchObject({ stage: "step" });
  });

  it.each(["throws", "rejects", "pending"])(
    "does not wait for a reporter that %s",
    async (behavior) => {
      installTestApi();
      wrapFetch(
        ({ body, method }) =>
          method === "PATCH" && body?.status === "completed",
      );
      const reporter = vi.fn(() => {
        if (behavior === "throws") throw new Error("reporter threw");
        if (behavior === "rejects") return Promise.reject("reporter rejected");
        return new Promise<void>(() => undefined);
      });
      const recorded = new KitaruAgent(agentFor(), {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        onRecordingError: reporter,
        requestedModelId: "lifecycle-model",
      });

      const output = await recorded.stream("hello");
      await expect(text(output)).resolves.toBe("done");
      await vi.waitFor(() => expect(reporter).toHaveBeenCalledTimes(1));
    },
  );

  it("emits a bounded default warning without request or transport details", async () => {
    installTestApi();
    wrapFetch(
      ({ body, method }) => method === "PATCH" && body?.status === "completed",
    );
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    const recorded = new KitaruAgent(agentFor(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });

    const output = await recorded.stream("prompt-with-secret-token");
    await text(output);
    await vi.waitFor(() => expect(warning).toHaveBeenCalledTimes(1));

    const rendered = warning.mock.calls.flat().join(" ");
    expect(rendered).toContain("Kitaru stream recording failed at complete");
    expect(rendered).not.toContain("prompt-with-secret-token");
    expect(rendered).not.toContain("capture failed");
  });

  it("retains final text beyond the old summary bound", async () => {
    const api = installTestApi();
    const answer = "x".repeat(8_192);
    const recorded = new KitaruAgent(agentFor(answer), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });

    const output = await recorded.stream("hello");
    await expect(text(output)).resolves.toBe(answer);
    expect(api.calls.at(-1)?.body?.outputs).toMatchObject({ text: answer });
  });

  it("degrades an oversized recording without altering the native result", async () => {
    const api = installTestApi();
    let runtimeOptions: RuntimeStreamOptions | undefined;
    const nativeResult = { native: true };
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        runtimeOptions = options;
        return nativeResult;
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "oversized-model",
    });
    const returned = await recorded.stream("hello");
    const oversized = "雪".repeat(1_048_600);

    await runtimeOptions?.onFinish?.({
      ...textStep("oversized"),
      steps: [],
      text: oversized,
      totalUsage: textStep("oversized").usage,
    });

    expect(returned).toBe(nativeResult);
    expect(api.calls.at(-1)?.body).toMatchObject({
      outputs: { kitaru_recording: "degraded", path: "run output" },
      status: "completed",
    });
  });

  it("redacts credential-shaped keys in structured completion output", async () => {
    const api = installTestApi();
    const nativeObject = {
      account: {
        api_key: "api-secret",
        profile: { password: "password-secret", visible: "kept" },
      },
      token: "token-secret",
    };
    const nativeResult = { object: nativeObject };
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
        const step = textStep("structured-secret");
        await options.onFinish?.({
          ...step,
          object: nativeObject,
          steps: [step],
          text: "",
          totalUsage: step.usage,
        } as never);
        return nativeResult;
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "structured-secret-model",
    });

    const returned = await recorded.stream("hello", {
      structuredOutput: { schema: {} },
    } as never);

    expect(returned).toBe(nativeResult);
    expect(returned.object).toBe(nativeObject);
    const expected = {
      account: {
        api_key: "[redacted]",
        profile: { password: "[redacted]", visible: "kept" },
      },
      token: "[redacted]",
    };
    expect(
      api
        .nodeBatches()
        .flat()
        .find((node) => node.name === "run" && node.status === "completed"),
    ).toMatchObject({ outputs: { object: expected } });
    expect(api.calls.at(-1)?.body).toMatchObject({
      outputs: { object: expected },
      status: "completed",
    });
  });

  it("preserves a native model failure and marks the recording failed", async () => {
    const api = installTestApi();
    enforceTerminalSessionTransitions();
    const modelError = new Error("native model stream failed");
    const callerError = vi.fn();
    const recorded = new KitaruAgent(failingAgent(modelError), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "failing-model",
    });

    const output = await recorded.stream("hello", { onError: callerError });
    await expect(text(output)).resolves.toBe("partial");
    await expect(output.getFullOutput()).rejects.toThrow(
      "native model stream failed",
    );
    await vi.waitFor(
      () =>
        expect(
          api.calls.some(
            (call) => call.method === "PATCH" && call.body?.status === "failed",
          ),
        ).toBe(true),
      { timeout: 3_000 },
    );
    expect(callerError).toHaveBeenCalledTimes(1);
    expect(callerError.mock.calls[0]?.[0].error).toBe(modelError);
    const calls = api.calls;
    const failedModelIndex = calls.findIndex(
      (call) =>
        call.method === "POST" &&
        Array.isArray(call.body?.nodes) &&
        call.body.nodes.some(
          (node) =>
            typeof node === "object" &&
            node !== null &&
            (node as Record<string, unknown>).node_type === "llm_call" &&
            (node as Record<string, unknown>).status === "failed",
        ),
    );
    const failedSessionIndex = calls.findIndex(
      (call) => call.method === "PATCH" && call.body?.status === "failed",
    );
    expect(failedModelIndex).toBeGreaterThan(-1);
    expect(failedSessionIndex).toBeGreaterThan(failedModelIndex);
    const failedModelNodes = calls[failedModelIndex]?.body?.nodes;
    const failedModel = (
      Array.isArray(failedModelNodes) ? failedModelNodes : []
    ).find(
      (node) =>
        typeof node === "object" &&
        node !== null &&
        (node as Record<string, unknown>).node_type === "llm_call",
    );
    expect(failedModel).toMatchObject({
      error: "Mastra stream failed",
      status: "failed",
    });
    expect(api.calls[failedSessionIndex]?.body).toMatchObject({
      error: "Mastra stream failed",
      status: "failed",
    });
    expect(
      api.calls.some(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toBe(false);
  });

  it.each([
    ["throws", "aggregate"],
    ["throws", "text"],
    ["rejects", "aggregate"],
    ["rejects", "text"],
  ] as const)(
    "matches native %s onError channels when %s is consumed first",
    async (behavior, first) => {
      vi.spyOn(console, "error").mockImplementation(() => {});
      const nativeModelError = new Error("native onError model failure");
      const nativeHookError = new Error("native onError hook failure");
      const nativeOnError = vi.fn((_event: { error: unknown }) => {
        if (behavior === "rejects") return Promise.reject(nativeHookError);
        throw nativeHookError;
      });
      const native = await failingAgent(nativeModelError).stream("hello", {
        onError: nativeOnError,
      });
      const nativeChannels = await errorChannels(native, first);

      const api = installTestApi();
      const wrappedModelError = new Error("wrapped onError model failure");
      const wrappedHookError = new Error("wrapped onError hook failure");
      const wrappedOnError = vi.fn((_event: { error: unknown }) => {
        if (behavior === "rejects") return Promise.reject(wrappedHookError);
        throw wrappedHookError;
      });
      const recorded = new KitaruAgent(failingAgent(wrappedModelError), {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "failing-model",
      });
      const wrapped = await recorded.stream("hello", {
        onError: wrappedOnError,
      });
      const wrappedChannels = await errorChannels(wrapped, first);

      expect(nativeChannels.aggregate).toBe(nativeHookError);
      expect(wrappedChannels.aggregate).toBe(wrappedHookError);
      expect(wrappedChannels.text).toBe(nativeChannels.text);
      expect(nativeOnError).toHaveBeenCalledTimes(2);
      expect(nativeOnError.mock.calls[0]?.[0].error).toBe(nativeModelError);
      expect(nativeOnError.mock.calls[1]?.[0].error).toMatchObject({
        message: nativeHookError.message,
        name: nativeHookError.name,
      });
      expect(wrappedOnError).toHaveBeenCalledTimes(2);
      expect(wrappedOnError.mock.calls[0]?.[0].error).toBe(wrappedModelError);
      expect(wrappedOnError.mock.calls[1]?.[0].error).toMatchObject({
        message: wrappedHookError.message,
        name: wrappedHookError.name,
      });
      await vi.waitFor(() =>
        expect(
          api.calls.filter(
            (call) => call.method === "PATCH" && call.body?.status === "failed",
          ),
        ).toHaveLength(1),
      );
      expect(api.calls.at(-1)?.body).toMatchObject({
        error: "Mastra stream failed",
        status: "failed",
      });
    },
  );

  it.each([
    {
      errorName: "AI_APICallError",
      expectedError: "Mastra stream failed (AI_APICallError)",
      scenario: "AI SDK",
    },
    {
      errorName: `A${"x".repeat(80)}Error`,
      expectedError: "Mastra stream failed",
      scenario: "oversized",
    },
  ])(
    "records a bounded category for $scenario error-only streams",
    async ({ errorName, expectedError }) => {
      const api = installTestApi();
      const modelError = Object.assign(new Error("Bearer private-token"), {
        name: errorName,
      });
      const nativeResult = { native: true };
      const agent = Object.assign(new FakeAgent(), {
        async stream(_messages: unknown, options: RuntimeStreamOptions = {}) {
          await options.onError?.({ error: modelError });
          return nativeResult;
        },
      });
      const callerError = vi.fn();
      const recorded = new KitaruAgent(agent, {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "timeout-model",
      });

      await expect(
        recorded.stream("private prompt", { onError: callerError }),
      ).resolves.toBe(nativeResult);
      await vi.waitFor(
        () =>
          expect(
            api.calls.filter(
              (call) =>
                call.method === "PATCH" && call.body?.status === "failed",
            ),
          ).toHaveLength(1),
        { timeout: 2_000 },
      );

      expect(callerError).toHaveBeenCalledWith({ error: modelError });
      expect(api.calls.at(-1)?.body).toMatchObject({
        error: expectedError,
        status: "failed",
      });
      expect(JSON.stringify(api.calls)).not.toContain("private-token");
    },
  );

  it("preserves native abort behavior and never overwrites failure with success", async () => {
    const api = installTestApi();
    const controller = new AbortController();
    const callerAbort = vi.fn();
    const recorded = new KitaruAgent(abortableAgent(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "abortable-model",
    });

    const output = await recorded.stream("hello", {
      abortSignal: controller.signal,
      onAbort: callerAbort,
    });
    const reader = output.textStream.getReader();
    await expect(reader.read()).resolves.toMatchObject({ value: "partial" });
    controller.abort(new Error("caller stopped"));
    await reader.read().catch(() => undefined);

    await vi.waitFor(() => expect(callerAbort).toHaveBeenCalledTimes(1));
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
    expect(
      api.calls.some(
        (call) => call.method === "PATCH" && call.body?.status === "completed",
      ),
    ).toBe(false);
  });

  it("records Mastra's background completion after the caller cancels early", async () => {
    const api = installTestApi();
    const recorded = new KitaruAgent(cancellableAgent(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "cancellable-model",
    });
    const output = await recorded.stream("hello");
    const reader = output.textStream.getReader();
    const cancellation = reader.cancel("stop reading");
    await cancellation;
    await vi.waitFor(() =>
      expect(
        api.calls.some(
          (call) =>
            call.method === "PATCH" && call.body?.status === "completed",
        ),
      ).toBe(true),
    );
  });

  it("rejects root initialization before model execution and preserves the setup error", async () => {
    installTestApi();
    let modelCalls = 0;
    const original = globalThis.fetch;
    wrapFetch(
      ({ method, path }) => method === "POST" && path.endsWith("/nodes"),
    );
    const agent = new Agent({
      id: "setup-agent",
      instructions: "Respond.",
      model: new MastraLanguageModelV2Mock({
        doStream: async () => {
          modelCalls += 1;
          return { stream: chunks("should not run") as never };
        },
        modelId: "setup-model",
        provider: "test-provider",
      }),
      name: "Setup",
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "setup-model",
    });

    await expect(recorded.stream("hello")).rejects.toThrow("capture failed");
    expect(modelCalls).toBe(0);
    expect(globalThis.fetch).not.toBe(original);
  });

  it("captures real recalled context before initialization failure prevents model execution", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const memory = new StreamHistoryMemory();
    const threadId = "stream-context";
    const resourceId = "resource";
    await memory.createThread({ resourceId, threadId });
    await memory.saveMessages({
      messages: [
        {
          content: {
            format: 2,
            parts: [{ text: "remember blue", type: "text" }],
          },
          createdAt: new Date("2026-01-01T00:00:00Z"),
          id: "remembered",
          resourceId,
          role: "user",
          threadId,
        },
      ],
    });
    const api = installTestApi();
    wrapFetch(
      ({ method, path }) => method === "POST" && path.endsWith("/nodes"),
    );
    const recall = vi.spyOn(memory.domain, "listMessages");
    let modelCalls = 0;
    const agent = new Agent({
      id: "memory-setup-agent",
      instructions: "Respond.",
      memory,
      model: new MastraLanguageModelV2Mock({
        doStream: async () => {
          modelCalls += 1;
          return { stream: chunks("should not run") as never };
        },
        modelId: "memory-model",
        provider: "test-provider",
      }),
      name: "Memory setup",
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "memory-model",
    });

    const callerError = vi.fn();

    const output = await recorded.stream("which color?", {
      memory: { resource: resourceId, thread: threadId },
      onError: callerError,
    });
    const channels = await errorChannels(output, "aggregate");
    expect(channels.aggregate).toMatchObject({
      message: expect.stringContaining("capture failed"),
    });
    expect(channels.text).not.toBe("timeout");
    expect(callerError).toHaveBeenCalled();
    expect(recall).toHaveBeenCalled();
    expect(modelCalls).toBe(0);
    const recordedInput = api.calls.find(
      (call) => call.method === "POST" && call.path === "/api/v1/sessions",
    )?.body?.inputs;
    expect(recordedInput).toMatchObject({
      mastra_conversation_context: { complete: true, source: "recalled" },
      supplied_messages: "which color?",
    });
    expect(JSON.stringify(recordedInput)).toContain("remember blue");
  });

  it("rejects session creation before model execution", async () => {
    installTestApi();
    wrapFetch(
      ({ method, path }) => method === "POST" && path === "/api/v1/sessions",
    );
    let modelCalls = 0;
    const agent = new Agent({
      id: "create-failure",
      instructions: "Respond.",
      model: new MastraLanguageModelV2Mock({
        doStream: async () => {
          modelCalls += 1;
          return { stream: chunks("should not run") as never };
        },
        modelId: "create-model",
        provider: "test-provider",
      }),
      name: "Create failure",
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "create-model",
    });

    await expect(recorded.stream("hello")).rejects.toThrow("capture failed");
    expect(modelCalls).toBe(0);
  });

  it("preserves a native startup error and fails the initialized session once", async () => {
    const api = installTestApi();
    const startupError = new Error("native startup failed");
    const agent = Object.assign(new FakeAgent(), {
      async stream(_messages: unknown): Promise<never> {
        throw startupError;
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "startup-model",
    });

    const caught = await recorded.stream("hello").catch((error) => error);

    expect(caught).toBe(startupError);
    expect(
      api.calls.filter(
        (call) => call.method === "PATCH" && call.body?.status === "failed",
      ),
    ).toHaveLength(1);
  });

  it("does not recategorize a caller finish-hook failure", async () => {
    const api = installTestApi();
    const onRecordingError = vi.fn();
    const hookError = new Error("caller finish failed");
    const recorded = new KitaruAgent(agentFor(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onRecordingError,
      requestedModelId: "lifecycle-model",
    });
    const output = await recorded.stream("hello", {
      onFinish: () => {
        throw hookError;
      },
    });

    const aggregate = await output.getFullOutput().catch((error) => error);
    expect(aggregate).toMatchObject({ cause: hookError });
    expect(onRecordingError).not.toHaveBeenCalled();
    expect(api.calls.at(-1)?.body).toMatchObject({
      error: "Mastra stream failed",
      status: "failed",
    });
  });

  it("matches native aggregate and text-reader channels for finish-hook errors", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const nativeError = new Error("native baseline finish hook");
    const native = await agentFor().stream("hello", {
      onFinish: () => {
        throw nativeError;
      },
    });
    const nativeAggregate = await native
      .getFullOutput()
      .catch((error) => error);
    const nativeText = await Promise.race([
      text(native).then(
        () => "resolved",
        () => "rejected",
      ),
      new Promise<"timeout">((resolve) =>
        setTimeout(() => resolve("timeout"), 50),
      ),
    ]);

    installTestApi();
    const wrappedError = new Error("wrapped finish hook");
    const recorded = new KitaruAgent(agentFor(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "lifecycle-model",
    });
    const wrapped = await recorded.stream("hello", {
      onFinish: () => {
        throw wrappedError;
      },
    });
    const wrappedAggregate = await wrapped
      .getFullOutput()
      .catch((error) => error);
    const wrappedText = await Promise.race([
      text(wrapped).then(
        () => "resolved",
        () => "rejected",
      ),
      new Promise<"timeout">((resolve) =>
        setTimeout(() => resolve("timeout"), 50),
      ),
    ]);

    expect(wrappedAggregate.id).toBe(nativeAggregate.id);
    expect(wrappedAggregate.cause).toBe(wrappedError);
    expect(nativeAggregate.cause).toBe(nativeError);
    expect(wrappedText).toBe(nativeText);
  });

  it("does not recategorize a caller step-hook failure", async () => {
    const api = installTestApi();
    const onRecordingError = vi.fn();
    const hookError = new Error("caller step failed");
    const recorded = new KitaruAgent(agentFor(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onRecordingError,
      requestedModelId: "lifecycle-model",
    });
    const output = await recorded.stream("hello", {
      onStepFinish: () => {
        throw hookError;
      },
    });

    const aggregate = await output.getFullOutput().catch((error) => error);
    expect(aggregate).toMatchObject({ cause: hookError });
    expect(onRecordingError).not.toHaveBeenCalled();
    expect(api.calls.at(-1)?.body).toMatchObject({
      error: "Mastra stream failed",
      status: "failed",
    });
  });
});
