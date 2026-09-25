import { Agent } from "@mastra/core/agent";
import { MessageList } from "@mastra/core/agent/message-list";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { KitaruClient } from "@zenml-io/kitaru";
import { RunRecorder } from "@zenml-io/kitaru/adapter";
import { APICallError } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { decodeMemoryValue } from "../src/memory-snapshot.js";
import { createRequestCapture } from "../src/request-capture.js";
import { recordStep } from "../src/step-recorder.js";
import { type ModelCall, textStream } from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, textStep } from "./helpers.js";

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Expected recorded evidence");
  return value;
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it.each(["v2", "v3", "v4"])(
  "preserves %s model receiver, arguments, result and unread stream",
  async (version) => {
    const result = textStream("native");
    const received: unknown[] = [];
    class Model {
      #id = "private-model";
      specificationVersion = version;
      provider = "fixture";
      get modelId() {
        return this.#id;
      }
      async doStream(args: unknown) {
        received.push(args);
        return result;
      }
      async doGenerate(args: unknown) {
        received.push(args);
        return { text: this.#id };
      }
    }
    const capture = createRequestCapture({
      invocationId: "invocation",
      getMemoryRevision: () => 4,
    });
    capture.beginStep({ stepNumber: 2 });
    const model = capture.instrumentModel(new Model());
    const args = {
      prompt: [
        {
          role: "user",
          content: [
            {
              type: "file",
              data: new Uint8Array([1, 0, 255]),
              mediaType: "image/png",
            },
          ],
        },
      ],
      tools: [
        { type: "function", name: "local", inputSchema: { type: "object" } },
      ],
      toolChoice: { type: "auto" },
      temperature: 0.4,
      headers: { authorization: "SECRET" },
      abortSignal: new AbortController().signal,
    };
    expect(await model.doStream(args)).toBe(result);
    expect(result.stream.locked).toBe(false);
    expect(received[0]).toBe(args);
    const evidence = required(capture.takeSuccessful(2));
    expect(evidence).toMatchObject({
      invocationId: "invocation",
      stepNumber: 2,
      attemptNumber: 1,
      memoryRevision: 4,
      modelId: "private-model",
      complete: true,
    });
    expect(decodeMemoryValue(evidence.inputs)).toEqual({
      prompt: args.prompt,
      tools: args.tools,
      toolChoice: args.toolChoice,
    });
    expect(evidence.modelSettings).toEqual({ temperature: 0.4 });
    expect(JSON.stringify(evidence)).not.toContain("SECRET");
    expect(capture.takeSuccessful(2)).toBeUndefined();
    expect(await model.doGenerate({ prompt: [] })).toEqual({
      text: "private-model",
    });
    expect(capture.takeSuccessful(2)).toMatchObject({
      attemptNumber: 2,
      method: "doGenerate",
    });
  },
);

it("captures retries after late prompt and settings changes without changing the provider arguments", async () => {
  const received: ModelCall[] = [];
  const requestIds: (string | undefined)[] = [];
  const failed = vi.fn();
  const capture = createRequestCapture({
    invocationId: "retry",
    getMemoryRevision: () => 3,
    onFailedAttempt: failed,
  });
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async (args) => {
      received.push(args);
      requestIds.push(capture.currentRequestId);
      if (received.length === 1)
        throw new APICallError({
          message: "Retry",
          url: "https://fixture.invalid",
          requestBodyValues: {},
          statusCode: 503,
          isRetryable: true,
        });
      return textStream("done");
    },
  });
  const agent = new Agent({
    id: "capture",
    name: "capture",
    instructions: "APP",
    model: capture.instrumentModel(model),
    inputProcessors: [
      {
        id: "late",
        processInputStep: ({ stepNumber, messageList }) => {
          capture.beginStep({ stepNumber, messageList });
          return { modelSettings: { temperature: 0.7, maxRetries: 1 } };
        },
        processLLMRequest: ({ prompt }) => ({
          prompt: [...prompt, { role: "system", content: "LATE" }],
        }),
      },
    ],
  });
  const output = await agent.stream("question");
  await output.consumeStream();
  expect(requestIds).toHaveLength(2);
  expect(typeof requestIds[0]).toBe("string");
  expect(requestIds[0]).not.toBe(requestIds[1]);
  expect(await output.text).toBe("done");
  await capture.drain();
  expect(failed).toHaveBeenCalledTimes(1);
  const first = required(failed.mock.calls[0])?.[0];
  const second = required(capture.takeSuccessful());
  expect(first.externalId).not.toBe(second.externalId);
  expect([first.attemptNumber, second.attemptNumber]).toEqual([1, 2]);
  for (const [index, evidence] of [first, second].entries()) {
    expect(requestIds[index]).toBe(evidence.externalId);
    expect(decodeMemoryValue(evidence.inputs)).toEqual({
      prompt: required(received[index]).prompt,
      tools: required(received[index]).tools,
      toolChoice: required(received[index]).toolChoice,
    });
    expect(evidence.modelSettings.temperature).toBe(0.7);
    expect(JSON.stringify(evidence.inputs)).toContain("LATE");
  }
}, 10000);

it("uses public tags and message sources for provenance", () => {
  const messageList = new MessageList();
  messageList.addSystem("application", "application");
  messageList.addSystem("remembered", "memory");
  messageList.add([{ role: "user", content: "old" }], "memory");
  messageList.add([{ role: "user", content: "extra" }], "context");
  const serialize = vi.spyOn(messageList, "serializeForSpan");
  const capture = createRequestCapture({
    invocationId: "sources",
    getMemoryRevision: () => 0,
  });
  capture.beginStep({ stepNumber: 0, messageList });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async () => ({}),
  });
  return model.doGenerate().then(() => {
    const evidence = required(capture.takeSuccessful());
    expect(evidence.provenance).toMatchObject({
      systemMessages: [{ tag: "application" }, { tag: "memory" }],
      messages: [{ source: "memory" }, { source: "context" }],
    });
    expect(JSON.stringify(evidence.provenance)).not.toContain(
      '"content":"old"',
    );
    expect(JSON.stringify(evidence.provenance)).not.toContain(
      '"content":"extra"',
    );
    expect(serialize).not.toHaveBeenCalled();
  });
});

it("keeps a 500 KiB actor request complete without copying its text into provenance", async () => {
  const capture = createRequestCapture({
    invocationId: "large-request",
    getMemoryRevision: () => 0,
  });
  const list = new MessageList();
  const content = "x".repeat(500 * 1024);
  list.add([{ role: "user", content }], "memory");
  capture.beginStep({
    stepNumber: 0,
    messageList: list,
    extraContext: { context: content },
  });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async (_args: unknown) => "native",
  });
  await model.doGenerate({ prompt: [{ role: "user", content }] });
  const evidence = required(capture.takeSuccessful());
  expect(evidence.complete).toBe(true);
  expect(JSON.stringify(evidence.inputs)).toContain(content);
  expect(JSON.stringify(evidence.provenance).length).toBeLessThan(1_000);
});

it("contains capture and telemetry failures while preserving native errors", async () => {
  const error = new Error("provider failed");
  const onCaptureError = vi.fn(() => {
    throw new Error("diagnostic failure");
  });
  const capture = createRequestCapture({
    invocationId: "failure",
    getMemoryRevision: () => {
      throw new Error("revision failed");
    },
    onFailedAttempt: async () => {
      throw new Error("write failed");
    },
    onCaptureError,
  });
  capture.beginStep({ stepNumber: 0 });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doStream: async (_args: unknown) => {
      throw error;
    },
  });
  await expect(model.doStream({ prompt: [] })).rejects.toBe(error);
  await expect(capture.drain()).resolves.toBeUndefined();
  expect(onCaptureError).toHaveBeenCalled();
});

it("marks unsupported or bounded evidence incomplete without failing native calls", async () => {
  const capture = createRequestCapture({
    invocationId: "bounded",
    getMemoryRevision: () => 0,
    recordingLimits: { maxStringChars: 10 },
  });
  capture.beginStep({ stepNumber: 0 });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async (_args: unknown) => "native",
  });
  await expect(
    model.doGenerate({
      prompt: [{ role: "system", content: "a".repeat(100) }],
      tools: [],
      toolChoice: { type: "auto" },
    }),
  ).resolves.toBe("native");
  const evidence = required(capture.takeSuccessful());
  expect(evidence).toMatchObject({
    complete: false,
    reasons: [],
    truncationReasons: [
      "Effective model request exceeds the configured recordingLimits and was truncated",
    ],
  });
  expect(JSON.stringify(evidence.inputs)).toContain("aaaaaaaaaa[truncated]");
});

it("keeps a request over 1 MiB complete and degrades one over the replay budget with its bound", async () => {
  const onCaptureError = vi.fn();
  const capture = createRequestCapture({
    invocationId: "budget",
    getMemoryRevision: () => 0,
    onCaptureError,
  });
  capture.beginStep({ stepNumber: 0, messageList: new MessageList() });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async (_args: unknown) => "native",
  });
  const content = "x".repeat(2 * 1_048_576);
  await model.doGenerate({ prompt: [{ role: "user", content }] });
  const large = required(capture.takeSuccessful());
  expect(large).toMatchObject({
    complete: true,
    reasons: [],
    truncationReasons: [],
  });
  expect(JSON.stringify(large.inputs)).toContain(content);
  await expect(
    model.doGenerate({
      prompt: [
        {
          role: "user",
          content: Array.from({ length: 210_000 }, () => ({
            type: "text",
            text: "",
          })),
        },
      ],
    }),
  ).resolves.toBe("native");
  const overflow = required(capture.takeSuccessful());
  expect(overflow).toMatchObject({
    complete: false,
    reasons: [],
    truncationReasons: [
      "Effective model request exceeds maximum item count 200000",
    ],
    inputs: {
      kitaru_recording: "degraded",
      path: "Effective model request",
      reason: "Effective model request exceeds maximum item count 200000",
    },
  });
  expect(onCaptureError).not.toHaveBeenCalled();
});

it("retains unfinished calls and captures independently replaced models", async () => {
  const capture = createRequestCapture({
    invocationId: "unfinished",
    getMemoryRevision: () => 1,
  });
  for (const stepNumber of [0, 1]) {
    capture.beginStep({ stepNumber });
    const original = {
      specificationVersion: "v2",
      modelId: `model-${stepNumber}`,
      provider: "fixture",
      doStream: async () => textStream("unread"),
    };
    const model = capture.instrumentModel(original);
    expect(capture.instrumentModel(model)).toBe(model);
    await model.doStream();
  }
  expect(capture.flushUnfinished()).toMatchObject([
    { stepNumber: 0, modelId: "model-0", attemptNumber: 1 },
    { stepNumber: 1, modelId: "model-1", attemptNumber: 1 },
  ]);
  expect(capture.flushUnfinished()).toEqual([]);
  expect(capture.takeSuccessful()).toBeUndefined();
});

it("excludes nested transport metadata and credentials from provider settings", async () => {
  const capture = createRequestCapture({
    invocationId: "redaction",
    getMemoryRevision: () => 0,
  });
  capture.beginStep({ stepNumber: 0 });
  const model = capture.instrumentModel({
    specificationVersion: "v3",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async (_args: unknown) => "native",
  });
  await model.doGenerate({
    prompt: [],
    providerOptions: {
      vendor: { headers: { "x-custom-access": "SECRET_HEADER" } },
    },
  });
  const evidence = required(capture.takeSuccessful());
  expect(evidence.complete).toBe(false);
  expect(JSON.stringify(evidence)).not.toContain("SECRET_HEADER");
  await model.doGenerate({
    prompt: [],
    providerOptions: { vendor: { apiKey: "SECRET_KEY" } },
  });
  const credentials = required(capture.takeSuccessful());
  expect(credentials.complete).toBe(false);
  expect(JSON.stringify(credentials)).not.toContain("SECRET_KEY");
});

it("keeps ordinary multi-kilobyte prompts complete within replay payload bounds", async () => {
  const capture = createRequestCapture({
    invocationId: "long",
    getMemoryRevision: () => 0,
  });
  capture.beginStep({ stepNumber: 0 });
  const model = capture.instrumentModel({
    specificationVersion: "v2",
    modelId: "actor",
    provider: "fixture",
    doGenerate: async (_args: unknown) => "native",
  });
  const prompt = [{ role: "system", content: "x".repeat(10000) }];
  await model.doGenerate({ prompt });
  const evidence = required(capture.takeSuccessful());
  expect(evidence.complete).toBe(true);
  expect(decodeMemoryValue(evidence.inputs)).toMatchObject({ prompt });
});

it("enriches the ordinary LLM node without duplicating nodes or changing legacy input", async () => {
  const api = installTestApi();
  const recorder = await RunRecorder.create({
    adapterVersion: "test",
    agentId: AGENT_ID,
    client: new KitaruClient({ apiUrl: "https://api.example" }),
    effectiveInput: "input",
    framework: "mastra",
    requestedModelId: "requested",
  });
  const capture = createRequestCapture({
    invocationId: "enrich",
    getMemoryRevision: () => 7,
  });
  capture.beginStep({ stepNumber: 0 });
  await capture
    .instrumentModel({
      specificationVersion: "v2",
      provider: "fixture",
      modelId: "actor",
      doGenerate: async (_args: unknown) => ({}),
    })
    .doGenerate({ prompt: [{ role: "user", content: [] }], temperature: 0.6 });
  const evidence = required(capture.takeSuccessful());
  await recordStep(
    recorder.state,
    textStep("enriched"),
    undefined,
    undefined,
    evidence,
  );
  await recordStep(recorder.state, textStep("legacy"));
  const nodes = api
    .nodeBatches()
    .flat()
    .filter((node) => node.node_type === "llm_call");
  expect(nodes).toHaveLength(2);
  expect(nodes[0]).toMatchObject({
    external_id: evidence.externalId,
    inputs: evidence.inputs,
    model_params: { temperature: 0.6 },
    attributes: {
      invocation_id: "enrich",
      memory_revision: 7,
      attempt_number: 1,
    },
  });
  expect(nodes[1]).toMatchObject({
    external_id: "response-legacy",
    inputs: null,
  });
});
