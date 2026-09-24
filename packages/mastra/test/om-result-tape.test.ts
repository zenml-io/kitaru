import { expect, it, vi } from "vitest";
import { decodeMemoryValue } from "../src/memory-snapshot.js";
import {
  createOMResultTape,
  getNativeOMModel,
  MastraOMDivergenceError,
} from "../src/om-result-tape.js";

function model() {
  return {
    specificationVersion: "v2",
    modelId: "fixture",
    provider: "fixture",
    doStream: vi.fn(async (_input: unknown) => ({
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: "text-delta", textDelta: "remembered" });
          controller.enqueue({ type: "finish", finishReason: "stop" });
          controller.close();
        },
      }),
    })),
  };
}

async function collect(stream: ReadableStream<unknown>): Promise<unknown[]> {
  const chunks: unknown[] = [];
  const reader = stream.getReader();
  while (true) {
    const item = await reader.read();
    if (item.done) break;
    chunks.push(item.value);
  }
  reader.releaseLock();
  return chunks;
}

it("reuses ordered recorded OM output without calling the live model", async () => {
  const capture = createOMResultTape(undefined, () => {
    throw new Error("unexpected incomplete result");
  });
  const native = model();
  const baseline = capture.instrument(native, "observer");
  const output = await baseline.doStream({ prompt: "before" });
  expect(await collect(output.stream)).toHaveLength(2);
  const entries = await capture.finish();
  expect(entries).toMatchObject([{ phase: "observer", ordinal: 0 }]);
  const live = model();
  const mismatch = vi.fn();
  const replay = createOMResultTape(entries, () => {}, mismatch);
  const recorded = replay.instrument(live, "observer");
  const replayOutput = await recorded.doStream({ prompt: "after" });
  expect(await collect(replayOutput.stream)).toEqual(
    await collect((await native.doStream({ prompt: "before" })).stream),
  );
  expect(live.doStream).not.toHaveBeenCalled();
  expect(mismatch).toHaveBeenCalledTimes(1);
  await replay.finish();
});

it("serializes an instrumented OM model as its configured value", () => {
  const tape = createOMResultTape(undefined, () => {});
  const fromId = tape.instrument(model(), "observer", "fixture/observer");
  expect(JSON.stringify({ model: fromId })).toBe(
    '{"model":"fixture/observer"}',
  );
  expect(getNativeOMModel(fromId)).toBe("fixture/observer");
  const configured = model();
  const fromObject = tape.instrument(configured, "reflector");
  expect(JSON.parse(JSON.stringify(fromObject))).toEqual(
    JSON.parse(JSON.stringify(configured)),
  );
  expect(getNativeOMModel(fromObject)).toBe(configured);
  expect(getNativeOMModel(configured)).toBe(configured);
});

it("rejects an extra or missing OM call", async () => {
  const capture = createOMResultTape(undefined, () => {});
  const output = await capture.instrument(model(), "reflector").doStream({});
  await collect(output.stream);
  const entries = await capture.finish();
  const extra = createOMResultTape(entries, () => {});
  const instrumented = extra.instrument(model(), "reflector");
  await collect((await instrumented.doStream({})).stream);
  await expect(instrumented.doStream({})).rejects.toBeInstanceOf(
    MastraOMDivergenceError,
  );
  const missing = createOMResultTape(entries, () => {});
  await expect(missing.finish()).rejects.toBeInstanceOf(
    MastraOMDivergenceError,
  );
});

it("captures non-JSON OM stream chunks without encoding codec tags twice", async () => {
  const chunks = [
    {
      date: new Date("2026-09-24T00:00:00.000Z"),
      absent: undefined,
      bytes: new Uint8Array([1, 2, 3]),
    },
  ];
  const tape = createOMResultTape(undefined, () => {
    throw new Error("unexpected incomplete result");
  });
  const wrapped = tape.instrument(
    {
      doStream: async (_input: unknown) => ({
        stream: new ReadableStream({
          start(controller) {
            for (const chunk of chunks) controller.enqueue(chunk);
            controller.close();
          },
        }),
      }),
    },
    "observer",
  );
  const output = await wrapped.doStream({});
  expect(await collect(output.stream)).toEqual(chunks);
  const entries = await tape.finish();
  expect(decodeMemoryValue(entries[0]?.output ?? null)).toEqual(chunks);
});

it("stops oversized OM capture while preserving every native stream chunk", async () => {
  const total = 10;
  let produced = 0;
  const onIncomplete = vi.fn();
  const tape = createOMResultTape(undefined, onIncomplete);
  const wrapped = tape.instrument(
    {
      doStream: async (_input: unknown) => ({
        stream: new ReadableStream(
          {
            pull(controller) {
              if (produced === total) {
                controller.close();
              } else {
                controller.enqueue(
                  Array.from({ length: 60_000 }, (_, index) => index),
                );
                produced += 1;
              }
            },
          },
          { highWaterMark: 0 },
        ),
      }),
    },
    "observer",
  );
  const output = await wrapped.doStream({});
  await expect(tape.finish()).rejects.toThrow(/incomplete/);
  expect(onIncomplete).toHaveBeenCalledTimes(1);
  expect(produced).toBeLessThan(total);
  const native = await collect(output.stream);
  expect(native).toHaveLength(total);
  expect((native[9] as number[])[59_999]).toBe(59_999);
});

it("stops OM capture at the aggregate byte limit without truncating native output", async () => {
  const chunk = "x".repeat(1_048_576);
  const total = 20;
  let produced = 0;
  const onIncomplete = vi.fn();
  const tape = createOMResultTape(undefined, onIncomplete);
  const wrapped = tape.instrument(
    {
      doStream: async (_input: unknown) => ({
        stream: new ReadableStream(
          {
            pull(controller) {
              if (produced === total) {
                controller.close();
              } else {
                controller.enqueue(chunk);
                produced += 1;
              }
            },
          },
          { highWaterMark: 0 },
        ),
      }),
    },
    "observer",
  );
  const output = await wrapped.doStream({});
  await expect(tape.finish()).rejects.toThrow(/incomplete/);
  expect(onIncomplete).toHaveBeenCalledTimes(1);
  expect(produced).toBeLessThan(total);
  const native = await collect(output.stream);
  expect(native).toHaveLength(total);
  expect(native[total - 1]).toBe(chunk);
});
