import { Agent } from "@mastra/core/agent";
import type { MastraModelConfig } from "@mastra/core/llm";
import { KitaruClient } from "@zenml-io/kitaru";
import { RunRecorder } from "@zenml-io/kitaru/adapter";
import { afterEach, describe, expect, it, vi } from "vitest";

import { prepareStructuredOutputModel } from "../src/structured-output-model.js";
import { AGENT_ID, installTestApi } from "./helpers.js";

function streamOf(chunks: unknown[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

const finish = {
  type: "finish",
  finishReason: "stop",
  usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 },
};

async function setup(
  createStream: () =>
    | ReadableStream<unknown>
    | Promise<ReadableStream<unknown>>,
  version: "v2" | "v3" | "v4" = "v2",
) {
  const api = installTestApi();
  const client = new KitaruClient({ apiUrl: "https://api.example" });
  const recorder = await RunRecorder.create({
    adapterVersion: "test",
    agentId: AGENT_ID,
    client,
    effectiveInput: "input",
    framework: "mastra",
    requestedModelId: "parent",
  });
  // Version-specific chunks are supplied by each case; the fixture shares only
  // the public provider methods used by Mastra's model resolver.
  const config = {
    specificationVersion: version,
    provider: "test-provider",
    modelId: "secondary",
    supportedUrls: {},
    doGenerate: async () => {
      throw new Error("Unexpected doGenerate");
    },
    doStream: async () => ({ stream: await createStream() }),
  } as MastraModelConfig;
  const agent = new Agent({
    id: "stream-test",
    name: "stream-test",
    instructions: "Answer",
    model: config,
  });
  const onAttemptFinish = vi.fn();
  const wrapped = await prepareStructuredOutputModel({
    modelConfig: config,
    resolveModel: (modelConfig) => agent.getModel({ modelConfig }),
    getState: async () => recorder.state,
    onAttemptFinish,
  });
  const resolved = await agent.getModel({ modelConfig: wrapped });
  return {
    client,
    state: recorder.state,
    onAttemptFinish,
    nodes: () => api.nodeBatches().flat(),
    async open() {
      return Reflect.apply(resolved.doStream, resolved, [
        {
          prompt: [
            {
              role: "user",
              content: [{ type: "text", text: "Structure this" }],
            },
          ],
          temperature: 0.2,
        },
      ]) as Promise<{ stream: ReadableStream<unknown> }>;
    },
  };
}

async function collect(stream: ReadableStream<unknown>): Promise<unknown[]> {
  const chunks: unknown[] = [];
  const reader = stream.getReader();
  while (true) {
    const next = await reader.read();
    if (next.done) return chunks;
    chunks.push(next.value);
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("structured output provider streams", () => {
  it.each([
    "v3",
    "v4",
  ] as const)("preserves %s chunks and records nested usage before EOF", async (version) => {
    const chunks = [
      { type: "response-metadata", id: "response", modelId: "served" },
      { type: "text-delta", id: "text", delta: '{"ok":true}' },
      {
        type: "finish",
        finishReason: { unified: "stop", raw: "end_turn" },
        usage: {
          inputTokens: { total: 11, noCache: 6, cacheRead: 5, cacheWrite: 0 },
          outputTokens: { total: 8, text: 5, reasoning: 3 },
        },
      },
    ];
    const test = await setup(() => streamOf(chunks), version);
    expect(await collect((await test.open()).stream)).toEqual(chunks);
    expect(test.nodes()).toHaveLength(1);
    expect(test.nodes()[0]).toMatchObject({
      requested_model: "secondary",
      model: "served",
      external_id: "response",
      status: "completed",
      model_params: { temperature: 0.2 },
      tokens: {
        input_tokens: 11,
        output_tokens: 8,
        cached_input_tokens: 5,
        reasoning_tokens: 3,
      },
      outputs: { text: '{"ok":true}', finish_reason: "stop" },
    });
  });

  it("preserves non-enumerable symbols and getters on the stream result", async () => {
    const test = await setup(() => streamOf([finish]));
    const modelConfig = {
      specificationVersion: "v2",
      provider: "test-provider",
      modelId: "secondary",
      supportedUrls: {},
      doGenerate: async () => {
        throw new Error("Unexpected doGenerate");
      },
      doStream: async () => ({ stream: streamOf([finish]) }),
    } as MastraModelConfig;
    const agent = new Agent({
      id: "metadata-test",
      name: "metadata-test",
      instructions: "Answer",
      model: modelConfig,
    });
    const model = await agent.getModel();
    const metadataKey = Symbol("transport metadata fixture");
    const metadata = { transport: "fixture" };
    let getterReads = 0;
    const getMetadata = () => {
      getterReads += 1;
      return metadata;
    };
    const nativeResult = Object.defineProperties(
      { stream: streamOf([finish]) },
      {
        [metadataKey]: { value: metadata, enumerable: false },
        metadata: { get: getMetadata, enumerable: false },
      },
    );
    // Attach the fixture at the resolved public model boundary: Mastra may
    // attach transport metadata after the provider returns its own result.
    vi.spyOn(model, "doStream").mockResolvedValue(
      nativeResult as Awaited<ReturnType<typeof model.doStream>>,
    );
    const wrapped = await prepareStructuredOutputModel({
      modelConfig,
      resolveModel: () => model,
      getState: async () => test.state,
    });
    const resolved = await agent.getModel({ modelConfig: wrapped });
    const result = await Reflect.apply(resolved.doStream, resolved, [
      { prompt: [] },
    ]);
    expect(getterReads).toBe(0);
    expect(Object.getOwnPropertyDescriptor(result, metadataKey)).toEqual(
      Object.getOwnPropertyDescriptor(nativeResult, metadataKey),
    );
    expect(Reflect.get(result, metadataKey)).toBe(metadata);
    expect(Object.getOwnPropertyDescriptor(result, "metadata")?.get).toBe(
      getMetadata,
    );
    expect(Reflect.get(result, "metadata")).toBe(metadata);
    expect(getterReads).toBe(1);
    expect(result.stream).not.toBe(nativeResult.stream);
    expect(await collect(result.stream)).toEqual([finish]);
    expect(test.nodes()).toHaveLength(1);
  });

  it("forwards error chunks and records the attempt as failed", async () => {
    const error = new Error("Provider rejected response");
    const chunks = [
      { type: "error", error },
      { ...finish, finishReason: "error" },
    ];
    const test = await setup(() => streamOf(chunks));
    const received = await collect((await test.open()).stream);
    expect(received).toEqual(chunks);
    expect(received[0]).toBe(chunks[0]);
    expect(test.nodes()).toHaveLength(1);
    expect(test.nodes()[0]).toMatchObject({
      status: "failed",
      error: error.message,
    });
  });

  it("preserves reader rejection and records partial text", async () => {
    const error = new Error("Connection lost");
    let reads = 0;
    const test = await setup(
      () =>
        new ReadableStream(
          {
            pull(controller) {
              if (reads++ === 0)
                controller.enqueue({
                  type: "text-delta",
                  id: "text",
                  delta: "partial",
                });
              else controller.error(error);
            },
          },
          { highWaterMark: 0 },
        ),
    );
    const reader = (await test.open()).stream.getReader();
    expect((await reader.read()).value).toMatchObject({ delta: "partial" });
    await expect(reader.read()).rejects.toBe(error);
    expect(test.nodes()).toHaveLength(1);
    expect(test.nodes()[0]).toMatchObject({
      status: "failed",
      error: error.message,
      outputs: { text: "partial" },
    });
  });

  it("records EOF without a finish event as incomplete", async () => {
    const test = await setup(() => streamOf([]));
    expect(await collect((await test.open()).stream)).toEqual([]);
    expect(test.nodes()).toHaveLength(1);
    expect(test.nodes()[0]).toMatchObject({
      status: "failed",
      error: "Structured output stream ended without a finish event",
    });
  });

  it("records cancellation during a pending read exactly once", async () => {
    const cancellation = new Error("Consumer stopped");
    const cancel = vi.fn();
    let signalRead: () => void = () => {};
    const reading = new Promise<void>((resolve) => {
      signalRead = resolve;
    });
    const test = await setup(
      () =>
        new ReadableStream({
          pull() {
            signalRead();
          },
          cancel,
        }),
    );
    const reader = (await test.open()).stream.getReader();
    const pendingRead = reader.read();
    await reading;
    await reader.cancel(cancellation);
    expect(await pendingRead).toEqual({ done: true, value: undefined });
    expect(cancel).toHaveBeenCalledExactlyOnceWith(cancellation);
    expect(test.nodes()).toHaveLength(1);
    expect(test.nodes()[0]).toMatchObject({
      status: "failed",
      error: cancellation.message,
    });
  });

  it.each([
    false,
    true,
  ])("stores recording failure while preserving native stream failure=%s", async (failProvider) => {
    const providerError = new Error("Provider disconnected");
    const recordingError = new Error("Recorder unavailable");
    const test = await setup(() =>
      failProvider
        ? new ReadableStream({
            start(controller) {
              controller.error(providerError);
            },
          })
        : streamOf([finish]),
    );
    vi.spyOn(test.client, "upsertSessionNodes").mockRejectedValue(
      recordingError,
    );
    const result = collect((await test.open()).stream);
    if (failProvider) await expect(result).rejects.toBe(providerError);
    else expect(await result).toEqual([finish]);
    expect(test.state.failure).toBe(recordingError);
    await expect(test.state.awaitSteps()).rejects.toBe(recordingError);
  });

  it("records each explicitly repeated doStream attempt once", async () => {
    const failure = new Error("Retryable provider error");
    let attempts = 0;
    const test = await setup(() => {
      if (attempts++ === 0) throw failure;
      return streamOf([finish]);
    });
    await expect(test.open()).rejects.toBe(failure);
    expect(test.onAttemptFinish).toHaveBeenLastCalledWith(failure);
    expect(await collect((await test.open()).stream)).toEqual([finish]);
    expect(test.onAttemptFinish).toHaveBeenLastCalledWith(undefined);
    expect(test.nodes().map((node) => node.status)).toEqual([
      "failed",
      "completed",
    ]);
    expect(new Set(test.nodes().map((node) => node.index)).size).toBe(2);
    expect(attempts).toBe(2);
  });
});
