import type { SessionNodeCreateRequest } from "@zenml-io/kitaru";
import { expect, it, vi } from "vitest";
import {
  createProcessorDecisions,
  type ProcessorDecisionOptions,
} from "../src/processor-decisions.js";

function setup(options: Partial<ProcessorDecisionOptions> = {}) {
  const nodes: SessionNodeCreateRequest[] = [];
  const captureError = vi.fn();
  const capture = createProcessorDecisions({
    mode: "live",
    invocationId: "turn",
    recordNode: async (node) => {
      nodes.push(node);
    },
    captureError,
    ...options,
  });
  const decision = capture.binding.define("skill-router");
  capture.validatePinned();
  return { capture, decision, nodes, captureError };
}

it("preserves native result and error identities and calls each callback once", async () => {
  const { capture, decision, nodes } = setup();
  const result = { skill: "support" };
  const callback = vi.fn(() => result);
  expect(await decision.run(callback)).toBe(result);
  expect(callback).toHaveBeenCalledTimes(1);
  await capture.finish();
  expect(capture.snapshot()).toMatchObject({
    complete: true,
    entries: [{ name: "skill-router", output: result }],
  });
  expect(nodes).toMatchObject([
    { name: "skill-router", node_type: "span", status: "completed" },
  ]);
  const failing = setup();
  const error = new Error("native failure");
  const fail = vi.fn(() => {
    throw error;
  });
  await expect(failing.decision.run(fail)).rejects.toBe(error);
  await failing.capture.finish();
  expect(fail).toHaveBeenCalledTimes(1);
  expect(failing.nodes[0]).toMatchObject({
    status: "failed",
    error: "Processor decision failed",
  });
  expect(failing.capture.snapshot().complete).toBe(false);
});

it("pins complete decisions without running classifier callbacks", async () => {
  const recorded = setup();
  await recorded.decision.run(() => ({
    skill: "support",
    at: new Date("2026-09-30T12:00:00Z"),
  }));
  await recorded.capture.finish();
  const replay = setup({
    mode: "pinned",
    recorded: recorded.capture.snapshot(),
  });
  const callback = vi.fn(() => {
    throw new Error("must not run");
  });
  expect(await replay.decision.run(callback)).toEqual({
    skill: "support",
    at: new Date("2026-09-30T12:00:00Z"),
  });
  expect(callback).not.toHaveBeenCalled();
  await expect(replay.decision.run(callback)).rejects.toThrow("more than once");
  await replay.capture.finish();
});

it.each([
  undefined,
  { version: 1, complete: false, declared: ["skill-router"], entries: [] },
  {
    version: 1,
    complete: true,
    declared: ["other"],
    entries: [{ name: "other", output: 1 }],
  },
  { version: 1, complete: true, declared: ["skill-router"], entries: [] },
  {
    version: 1,
    complete: true,
    declared: ["skill-router"],
    entries: [{ name: "skill-router", output: { $mastra: "broken" } }],
  },
])(
  "rejects unavailable or damaged pinned decisions at preflight",
  (recorded) => {
    expect(() => setup({ mode: "pinned", recorded })).toThrow();
  },
);

it("records duplicate live decisions while refusing to make them pinnable", async () => {
  const { capture, decision, nodes } = setup();
  const callback = vi.fn(() => "skill");
  await decision.run(callback);
  await decision.run(callback);
  await capture.finish();
  expect(callback).toHaveBeenCalledTimes(2);
  expect(nodes).toHaveLength(2);
  expect(capture.snapshot().complete).toBe(false);
});

function model() {
  const result = {
    content: [{ type: "text", text: "support" }],
    finishReason: "stop",
    usage: {
      inputTokens: { total: 10, cacheRead: 2 },
      outputTokens: { total: 3, reasoning: 1 },
    },
    response: { modelId: "served" },
  };
  return {
    result,
    native: {
      modelId: "requested",
      provider: "openai.responses",
      specificationVersion: "v3",
      doGenerate: vi.fn(async (_input: unknown) => result),
    },
  };
}

it("captures classifier calls as decision children with usage, costs and model identity", async () => {
  const costCalculator = vi.fn(() => "0.001");
  const { capture, decision, nodes } = setup({ costCalculator });
  const { native, result } = model();
  const classifier = decision.instrumentModel(native);
  expect(
    await decision.run(() =>
      classifier.doGenerate({
        prompt: [{ role: "user", content: "message" }],
        headers: { authorization: "secret" },
      }),
    ),
  ).toBe(result);
  await capture.finish();
  const llm = nodes.find((node) => node.node_type === "llm_call");
  const span = nodes.find((node) => node.node_type === "span");
  expect(llm).toMatchObject({
    parent_external_id: span?.external_id,
    requested_model: "requested",
    model: "served",
    model_provider: "openai",
    cost: "0.001",
    tokens: {
      input_tokens: 10,
      output_tokens: 3,
      cached_input_tokens: 2,
      reasoning_tokens: 1,
    },
    inputs: [{ role: "user", content: "message" }],
    outputs: { content: result.content, finish_reason: "stop" },
  });
  expect(costCalculator).toHaveBeenCalledWith({
    model: "served",
    provider: "openai.responses",
    requestedModelId: "requested",
    tokens: llm?.tokens,
  });
  expect(JSON.stringify(nodes)).not.toContain("secret");
});

it("keeps provider errors intact and records failed classifier calls", async () => {
  const { capture, decision, nodes } = setup();
  const error = new Error("provider failed");
  const { native } = model();
  native.doGenerate.mockRejectedValue(error);
  const classifier = decision.instrumentModel(native);
  await expect(
    decision.run(() => classifier.doGenerate({ prompt: [] })),
  ).rejects.toBe(error);
  await capture.finish();
  expect(nodes.find((node) => node.node_type === "llm_call")).toMatchObject({
    status: "failed",
    error: "Processor decision failed",
  });
});

it("returns before slow writes and hanging costs and bounds finalization", async () => {
  const { capture, decision, captureError } = setup({
    recordNode: () => new Promise(() => {}),
    costCalculator: () => new Promise(() => {}),
  });
  const classifier = decision.instrumentModel(model().native);
  const result = await decision.run(async () => {
    await classifier.doGenerate({ prompt: [] });
    return "native";
  });
  expect(result).toBe("native");
  await capture.finish(5);
  expect(capture.snapshot().complete).toBe(false);
  expect(captureError).toHaveBeenCalled();
});

it("isolates failed writes and failing diagnostic callbacks from native execution", async () => {
  const { capture, decision } = setup({
    recordNode: async () => {
      throw new Error("sink failed");
    },
    captureError: () => {
      throw new Error("diagnostic failed");
    },
  });
  expect(await decision.run(() => "native")).toBe("native");
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("keeps concurrent turn decisions and classifier parents separate", async () => {
  const first = setup({ invocationId: "first" });
  const second = setup({ invocationId: "second" });
  const a = first.decision.instrumentModel(model().native);
  const b = second.decision.instrumentModel(model().native);
  await Promise.all([
    first.decision.run(async () => {
      await Promise.resolve();
      await a.doGenerate({ prompt: "a" });
      return "a";
    }),
    second.decision.run(async () => {
      await b.doGenerate({ prompt: "b" });
      return "b";
    }),
  ]);
  await Promise.all([first.capture.finish(), second.capture.finish()]);
  expect(first.capture.snapshot().entries[0]?.output).toBe("a");
  expect(second.capture.snapshot().entries[0]?.output).toBe("b");
  expect(
    first.nodes.find((node) => node.node_type === "llm_call")
      ?.parent_external_id,
  ).toContain("first:");
  expect(
    second.nodes.find((node) => node.node_type === "llm_call")
      ?.parent_external_id,
  ).toContain("second:");
});

it("preserves unsupported streams and makes the decision unpinnable", async () => {
  const { capture, decision } = setup();
  const output = { stream: new ReadableStream() };
  const native = {
    specificationVersion: "v3",
    doStream: vi.fn(async () => output),
  };
  const classifier = decision.instrumentModel(native);
  expect(
    await decision.run(async () => {
      expect(await classifier.doStream()).toBe(output);
      return "native";
    }),
  ).toBe("native");
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("passes through native execution when recording is disabled", async () => {
  const { capture, decision } = setup({ recordNode: undefined });
  const { native } = model();
  expect(decision.instrumentModel(native)).toBe(native);
  const result = new Map([["native", true]]);
  expect(await decision.run(() => result)).toBe(result);
  await capture.finish();
});

it("captures request and result evidence before native callers mutate it", async () => {
  let release: (() => void) | undefined;
  const barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  const { capture, decision, nodes } = setup({
    costCalculator: async () => {
      await barrier;
      return 0;
    },
  });
  const { native, result } = model();
  const prompt = [{ role: "user", content: "original" }];
  const classifier = decision.instrumentModel(native);
  const output = await decision.run(async () => {
    const response = await classifier.doGenerate({ prompt });
    const part = response.content[0];
    if (part) part.text = "mutated inside callback";
    return { skill: "original" };
  });
  const message = prompt[0];
  if (message) message.content = "mutated";
  output.skill = "mutated";
  result.usage.inputTokens.total = 999;
  release?.();
  await capture.finish();
  const llm = nodes.find((node) => node.node_type === "llm_call");
  expect(llm?.inputs).toEqual([{ role: "user", content: "original" }]);
  expect(llm?.outputs).toMatchObject({ content: [{ text: "support" }] });
  expect(llm?.tokens?.input_tokens).toBe(10);
  expect(nodes.find((node) => node.node_type === "span")?.outputs).toEqual({
    skill: "original",
  });
  expect(capture.snapshot().entries[0]?.output).toEqual({ skill: "original" });
});

it("keeps unsupported models and out-of-scope live calls native but unpinnable", async () => {
  const { capture, decision } = setup();
  const unsupported = {
    specificationVersion: "v1",
    doGenerate: vi.fn(async () => "native"),
  };
  expect(decision.instrumentModel(unsupported)).toBe(unsupported);
  const { native, result } = model();
  expect(
    await decision.instrumentModel(native).doGenerate({ prompt: [] }),
  ).toBe(result);
  expect(await decision.run(() => "native")).toBe("native");
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("blocks an instrumented classifier called outside pinned callbacks", async () => {
  const baseline = setup();
  await baseline.decision.run(() => "native");
  await baseline.capture.finish();
  const replay = setup({
    mode: "pinned",
    recorded: baseline.capture.snapshot(),
  });
  const { native } = model();
  await expect(
    replay.decision.instrumentModel(native).doGenerate({ prompt: [] }),
  ).rejects.toThrow("cannot call a live classifier");
  expect(native.doGenerate).not.toHaveBeenCalled();
  await replay.capture.finish();
});

it("rejects unsupported result capture while preserving the native object", async () => {
  const { capture, decision } = setup();
  const value = new Map([["skill", "support"]]);
  expect(await decision.run(() => value)).toBe(value);
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("does not write nodes after a timed-out cost calculator eventually resolves", async () => {
  let release: (() => void) | undefined;
  const barrier = new Promise<void>((resolve) => {
    release = resolve;
  });
  const { capture, decision, nodes } = setup({
    costCalculator: async () => {
      await barrier;
      return 0;
    },
  });
  const classifier = decision.instrumentModel(model().native);
  await decision.run(async () => {
    await classifier.doGenerate({ prompt: [] });
    return "skill";
  });
  await capture.finish(5);
  const count = nodes.length;
  const snapshot = capture.snapshot();
  release?.();
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(nodes).toHaveLength(count);
  expect(capture.snapshot()).toEqual(snapshot);
});

it("preserves native provider results whose diagnostic fields cannot be read", async () => {
  const { capture, decision } = setup();
  const result = {
    get usage(): never {
      throw new Error("unreadable usage");
    },
  };
  const classifier = decision.instrumentModel({
    specificationVersion: "v3",
    modelId: "fixture",
    provider: "fixture",
    doGenerate: async () => result,
  });
  expect(
    await decision.run(async () => {
      const native = await classifier.doGenerate();
      expect(native).toBe(result);
      return "native";
    }),
  ).toBe("native");
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it.each(["prompt", "temperature"])(
  "preserves provider execution when the diagnostic input %s getter throws",
  async (key) => {
    const { capture, decision } = setup();
    const { native, result } = model();
    const input = Object.defineProperty({}, key, {
      get() {
        throw new Error("diagnostic input failed");
      },
    });
    const classifier = decision.instrumentModel(native);
    expect(await decision.run(() => classifier.doGenerate(input))).toBe(result);
    expect(native.doGenerate).toHaveBeenCalledTimes(1);
    expect(native.doGenerate.mock.calls[0]?.[0]).toBe(input);
    await capture.finish();
    expect(capture.snapshot().complete).toBe(false);
  },
);

it("does not inspect an unused native stream getter during generate instrumentation", async () => {
  const { capture, decision } = setup();
  const { native, result } = model();
  const streamGetter = vi.fn(() => {
    throw new Error("unused doStream getter");
  });
  Object.defineProperty(native, "doStream", { get: streamGetter });
  const classifier = decision.instrumentModel(native);
  expect(await decision.run(() => classifier.doGenerate({ prompt: [] }))).toBe(
    result,
  );
  expect(streamGetter).not.toHaveBeenCalled();
  await capture.finish();
  expect(capture.snapshot().complete).toBe(true);
});

it("preserves native generation when the recording-only model specification getter throws", async () => {
  const { capture, decision } = setup();
  const { native, result } = model();
  Object.defineProperty(native, "specificationVersion", {
    get() {
      throw new Error("specification evidence failed");
    },
  });
  const classifier = decision.instrumentModel(native);
  expect(classifier).toBe(native);
  expect(await decision.run(() => classifier.doGenerate({ prompt: [] }))).toBe(
    result,
  );
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("preserves actual native method getter failures when the application calls them", async () => {
  const { capture, decision } = setup();
  const { native } = model();
  const error = new Error("native doGenerate getter failed");
  Object.defineProperty(native, "doGenerate", {
    get() {
      throw error;
    },
  });
  const classifier = decision.instrumentModel(native);
  await expect(
    decision.run(() => classifier.doGenerate({ prompt: [] })),
  ).rejects.toBe(error);
  await capture.finish();
});

it("records safe provider failure categories without echoed credentials", async () => {
  const { capture, decision, nodes } = setup();
  const error = Object.assign(
    new Error("Authorization: Bearer private-credential"),
    { statusCode: 401 },
  );
  const { native } = model();
  native.doGenerate.mockRejectedValue(error);
  const classifier = decision.instrumentModel(native);
  await expect(
    decision.run(() => classifier.doGenerate({ prompt: [] })),
  ).rejects.toBe(error);
  await capture.finish();
  expect(
    nodes.filter((node) => node.status === "failed").map((node) => node.error),
  ).toEqual([
    "HTTP 401: authentication failed",
    "HTTP 401: authentication failed",
  ]);
  expect(JSON.stringify(nodes)).not.toContain("private-credential");
});

it("marks lossy classifier diagnostic input unpinnable without changing generation", async () => {
  const { capture, decision } = setup();
  const { native, result } = model();
  const classifier = decision.instrumentModel(native);
  const prompt = new Map([["unsupported", "native"]]);
  expect(
    await decision.run(async () => {
      expect(await classifier.doGenerate({ prompt })).toBe(result);
      return "native";
    }),
  ).toBe("native");
  await capture.finish();
  expect(capture.snapshot().complete).toBe(false);
});

it("records ModelRouter normalized finish reasons without degrading valid provider content", async () => {
  const { capture, decision, nodes } = setup();
  const result = {
    content: [
      {
        type: "reasoning",
        text: "",
        providerMetadata: {
          openai: { itemId: "reasoning", encryptedReasoningContent: "fixture" },
        },
      },
      {
        type: "text",
        text: "support",
        providerMetadata: { openai: { itemId: "text" } },
      },
    ],
    finishReason: { unified: "stop", raw: undefined },
    usage: {
      inputTokens: { total: 10, cacheRead: 0 },
      outputTokens: { total: 3, reasoning: 1 },
    },
  };
  const classifier = decision.instrumentModel({
    specificationVersion: "v2",
    modelId: "gpt-5-nano",
    provider: "openai.responses",
    doGenerate: async (_input: unknown) => result,
  });
  expect(
    await decision.run(async () => {
      expect(await classifier.doGenerate({ prompt: [] })).toBe(result);
      return { skill: "support" };
    }),
  ).toEqual({ skill: "support" });
  await capture.finish();
  expect(nodes.find((node) => node.node_type === "llm_call")?.outputs).toEqual({
    content: result.content,
    finish_reason: "stop",
  });
  expect(capture.snapshot().complete).toBe(true);
});

it("keeps recovered failed classifier attempts pinnable when the decision succeeds", async () => {
  const { capture, decision, nodes } = setup();
  const { native, result } = model();
  const providerError = new Error("provider transient failure");
  native.doGenerate.mockRejectedValueOnce(providerError);
  const classifier = decision.instrumentModel(native);
  expect(
    await decision.run(async () => {
      try {
        await classifier.doGenerate({ prompt: [] });
      } catch (error) {
        expect(error).toBe(providerError);
      }
      expect(await classifier.doGenerate({ prompt: [] })).toBe(result);
      return { skill: "support" };
    }),
  ).toEqual({ skill: "support" });
  await capture.finish();
  expect(
    nodes
      .filter((node) => node.node_type === "llm_call")
      .map((node) => node.status),
  ).toEqual(["failed", "completed"]);
  expect(nodes.find((node) => node.status === "failed")?.outputs).toBeNull();
  expect(capture.snapshot().complete).toBe(true);
});
