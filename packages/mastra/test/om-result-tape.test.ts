import { expect, it, vi } from "vitest";
import { decodeMemoryValue } from "../src/memory-snapshot.js";
import {
  createOMResultTape,
  getNativeOMModel,
  getOMInputFingerprint,
  MastraOMDivergenceError,
  MastraOMRecordedFailureError,
  MastraOMSkippedCallError,
  type OMResultEntry,
} from "../src/om-result-tape.js";
import { fileReference } from "../src/stateful-files.js";

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

it("reuses recorded OM output without calling the live model", async () => {
  const capture = createOMResultTape(undefined, () => {
    throw new Error("unexpected incomplete result");
  });
  const native = model();
  const baseline = capture.instrument(native, "observer");
  const output = await baseline.doStream({ prompt: "before" });
  expect(await collect(output.stream)).toHaveLength(2);
  const { entries } = await capture.finish();
  expect(entries).toMatchObject([{ phase: "observer", ordinal: 0 }]);
  const live = model();
  const replay = createOMResultTape(entries, () => {});
  const recorded = replay.instrument(live, "observer");
  const replayOutput = await recorded.doStream({ prompt: "after" });
  expect(await collect(replayOutput.stream)).toEqual(
    await collect((await native.doStream({ prompt: "before" })).stream),
  );
  expect(live.doStream).not.toHaveBeenCalled();
  expect((await replay.finish()).divergence).toEqual({
    inputMismatches: 1,
    surplusCalls: 0,
    unusedResults: 0,
    liveCalls: 0,
  });
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
  const { entries } = await tape.finish();
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
  await expect(tape.finish()).rejects.toMatchObject({
    message: expect.stringMatching(/incomplete/),
    reason: "om_tape_incomplete",
  });
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

function answering(answer: (input: { prompt?: unknown }) => string) {
  return {
    doStream: vi.fn(async (input: unknown) => ({
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: "stream-start", warnings: [] });
          controller.enqueue({
            type: "text-delta",
            delta: answer(input as { prompt?: unknown }),
          });
          controller.enqueue({ type: "finish", finishReason: "stop" });
          controller.close();
        },
      }),
    })),
  };
}

async function text(output: { stream: ReadableStream<unknown> }) {
  return (await collect(output.stream))
    .map((chunk) =>
      (chunk as { type: string }).type === "text-delta"
        ? (chunk as { delta: string }).delta
        : "",
    )
    .join("");
}

async function recordCalls(
  calls: Array<{ phase: "observer" | "reflector"; prompt: string }>,
): Promise<OMResultEntry[]> {
  const tape = createOMResultTape(undefined, () => {
    throw new Error("unexpected incomplete result");
  });
  const models = {
    observer: tape.instrument(
      answering((input) => `observed ${String(input.prompt)}`),
      "observer",
    ),
    reflector: tape.instrument(
      answering((input) => `reflected ${String(input.prompt)}`),
      "reflector",
    ),
  };
  for (const call of calls)
    await collect(
      (await models[call.phase].doStream({ prompt: call.prompt })).stream,
    );
  return (await tape.finish()).entries;
}

it("leaves buffered calls outside the recorded windows unobserved", async () => {
  // A slow production observer merged three buffer rounds into its second call.
  const entries = await recordCalls([
    { phase: "observer", prompt: "message 1" },
    { phase: "observer", prompt: "messages 2-4" },
    { phase: "reflector", prompt: "observations" },
  ]);
  const liveObserver = answering(() => "live");
  const liveReflector = answering(() => "live");
  const replay = createOMResultTape(entries, () => {}, {
    isBuffered: () => true,
  });
  const observer = replay.instrument(liveObserver, "observer");
  const reflector = replay.instrument(liveReflector, "reflector");
  const answers: string[] = [];
  for (const prompt of [
    "message 1",
    "message 2",
    "messages 2-3",
    "messages 2-4",
    "message 5",
  ])
    answers.push(await text(await observer.doStream({ prompt })));
  expect(answers).toEqual([
    "observed message 1",
    // An earlier, smaller window gets no observation instead of a result
    // that describes messages the replay has not produced yet.
    "",
    "",
    "observed messages 2-4",
    "",
  ]);
  await expect(
    reflector.doStream({ prompt: "other observations" }),
  ).rejects.toBeInstanceOf(MastraOMSkippedCallError);
  expect(await text(await reflector.doStream({ prompt: "observations" }))).toBe(
    "reflected observations",
  );
  expect(liveObserver.doStream).not.toHaveBeenCalled();
  expect(liveReflector.doStream).not.toHaveBeenCalled();
  expect((await replay.finish()).divergence).toEqual({
    inputMismatches: 0,
    surplusCalls: 1,
    unusedResults: 0,
    liveCalls: 0,
  });
});

it("fails a blocking call closed once its phase's recorded results are used", async () => {
  const entries = await recordCalls([{ phase: "observer", prompt: "first" }]);
  const live = answering(() => "live");
  const replay = createOMResultTape(entries, () => {});
  const observer = replay.instrument(live, "observer");
  expect(await text(await observer.doStream({ prompt: "changed" }))).toBe(
    "observed first",
  );
  // An empty observation would drop the observed messages from context.
  await expect(observer.doStream({ prompt: "more" })).rejects.toBeInstanceOf(
    MastraOMDivergenceError,
  );
  expect(live.doStream).not.toHaveBeenCalled();
  await expect(replay.finish()).rejects.toBeInstanceOf(MastraOMDivergenceError);
});

it("calls the live model only for a blocking call with no recorded result", async () => {
  const entries = await recordCalls([{ phase: "observer", prompt: "first" }]);
  const bytes = new TextEncoder().encode("attached");
  const reference = fileReference({ bytes, mediaType: "text/plain" });
  const received: unknown[] = [];
  const live = answering((input) => {
    received.push(input.prompt);
    return "observed live";
  });
  const replay = createOMResultTape(entries, () => {}, {
    missingResults: "live",
    resolveFileReference: async () => ({ bytes, mediaType: "text/plain" }),
  });
  const observer = replay.instrument(live, "observer", "fixture/observer");
  expect(await text(await observer.doStream({ prompt: "first" }))).toBe(
    "observed first",
  );
  const prompt = [
    {
      role: "user",
      content: [
        { type: "file", data: new URL(reference), mediaType: "text/plain" },
      ],
    },
  ];
  expect(await text(await observer.doStream({ prompt }))).toBe("observed live");
  expect(live.doStream).toHaveBeenCalledTimes(1);
  // No provider can fetch a captured reference, so the live model gets bytes.
  expect(received).toEqual([
    [
      {
        role: "user",
        content: [{ type: "file", data: bytes, mediaType: "text/plain" }],
      },
    ],
  ]);
  const result = await replay.finish();
  expect(result.divergence.liveCalls).toBe(1);
  expect(result.liveCalls).toMatchObject([
    {
      phase: "observer",
      model: "fixture/observer",
      prompt,
      failed: false,
      captured: true,
      output: expect.any(Array),
    },
  ]);
});

it("matches recorded OM results by input rather than call order", async () => {
  const entries = await recordCalls([
    { phase: "observer", prompt: "first" },
    { phase: "observer", prompt: "second" },
    { phase: "observer", prompt: "second" },
  ]);
  const replay = createOMResultTape(entries, () => {});
  const observer = replay.instrument(
    answering(() => "live"),
    "observer",
  );
  const answers = [
    await text(await observer.doStream({ prompt: "second" })),
    await text(await observer.doStream({ prompt: "first" })),
    await text(await observer.doStream({ prompt: "second" })),
  ];
  expect(answers).toEqual([
    "observed second",
    "observed first",
    "observed second",
  ]);
  expect((await replay.finish()).divergence).toEqual({
    inputMismatches: 0,
    surplusCalls: 0,
    unusedResults: 0,
    liveCalls: 0,
  });
  const fewer = createOMResultTape(entries, () => {});
  await collect(
    (
      await fewer
        .instrument(
          answering(() => "live"),
          "observer",
        )
        .doStream({ prompt: "first" })
    ).stream,
  );
  expect((await fewer.finish()).divergence.unusedResults).toBe(2);
});

it("fails replay closed when a phase has no recorded result", async () => {
  const entries = await recordCalls([{ phase: "observer", prompt: "only" }]);
  const live = answering(() => "live");
  const replay = createOMResultTape(entries, () => {});
  await expect(
    replay.instrument(live, "reflector").doStream({ prompt: "x" }),
  ).rejects.toBeInstanceOf(MastraOMDivergenceError);
  expect(live.doStream).not.toHaveBeenCalled();
  await expect(replay.finish()).rejects.toBeInstanceOf(MastraOMDivergenceError);
  const malformed = createOMResultTape(
    [{ phase: "observer" } as unknown as OMResultEntry],
    () => {},
  );
  await expect(
    malformed.instrument(live, "observer").doStream({ prompt: "x" }),
  ).rejects.toBeInstanceOf(MastraOMDivergenceError);
  expect(live.doStream).not.toHaveBeenCalled();
});

it.each(["fail", "live"] as const)(
  "skips a buffered call of a phase with no recorded result (%s mode)",
  async (missingResults) => {
    const entries = await recordCalls([{ phase: "reflector", prompt: "only" }]);
    const live = answering(() => "live");
    const replay = createOMResultTape(entries, () => {}, {
      isBuffered: () => true,
      missingResults,
    });
    // The replay's extra step started a buffered observation production
    // never made; it observes nothing, as a skipped window does.
    expect(
      await text(
        await replay.instrument(live, "observer").doStream({ prompt: "x" }),
      ),
    ).toBe("");
    const reflectorEntries = await recordCalls([
      { phase: "observer", prompt: "only" },
    ]);
    const reflectorReplay = createOMResultTape(reflectorEntries, () => {}, {
      isBuffered: () => true,
      missingResults,
    });
    await expect(
      reflectorReplay.instrument(live, "reflector").doStream({ prompt: "x" }),
    ).rejects.toBeInstanceOf(MastraOMSkippedCallError);
    expect(live.doStream).not.toHaveBeenCalled();
    expect((await replay.finish()).divergence.surplusCalls).toBe(1);
    expect((await reflectorReplay.finish()).divergence).toMatchObject({
      surplusCalls: 1,
      liveCalls: 0,
    });
  },
);

it("records failed OM attempts so a baseline with a successful retry replays", async () => {
  const onIncomplete = vi.fn();
  const tape = createOMResultTape(undefined, onIncomplete);
  let attempts = 0;
  const flaky = answering(() => "observed after retry");
  const native = flaky.doStream;
  flaky.doStream = vi.fn(async (input: unknown) => {
    attempts += 1;
    if (attempts === 1)
      throw Object.assign(new Error("Rate limit exceeded"), {
        statusCode: 429,
        isRetryable: true,
      });
    if (attempts === 2)
      return {
        stream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: "stream-start", warnings: [] });
            controller.error(new Error("connection reset"));
          },
        }),
      };
    return native(input);
  });
  const observer = tape.instrument(flaky, "observer");
  // Mastra retries each failure with the same call options.
  const input = { prompt: "messages" };
  await expect(observer.doStream(input)).rejects.toThrow("Rate limit");
  await expect(
    collect((await observer.doStream(input)).stream),
  ).rejects.toThrow("connection reset");
  expect(await text(await observer.doStream(input))).toBe(
    "observed after retry",
  );
  const { entries } = await tape.finish();
  expect(onIncomplete).not.toHaveBeenCalled();
  expect(entries.map((entry) => entry.failed ?? false)).toEqual([
    true,
    true,
    false,
  ]);
  const live = answering(() => "live");
  const replay = createOMResultTape(entries, () => {});
  expect(
    await text(await replay.instrument(live, "observer").doStream(input)),
  ).toBe("observed after retry");
  expect(live.doStream).not.toHaveBeenCalled();
  expect((await replay.finish()).divergence).toEqual({
    inputMismatches: 0,
    surplusCalls: 0,
    unusedResults: 0,
    liveCalls: 0,
  });
  const failedOnly = createOMResultTape(entries.slice(0, 1), () => {});
  await expect(
    failedOnly.instrument(live, "observer").doStream(input),
  ).rejects.toBeInstanceOf(MastraOMRecordedFailureError);
  expect(live.doStream).not.toHaveBeenCalled();
});

it("ignores wall-clock values and generated ids in the OM input fingerprint", () => {
  const prompt = (at: Date, clock: string, day: string, id: string) => ({
    prompt: [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: `--- message boundary (${at.toISOString()}) ---\n${day}:\nUser (${clock}): remember green (${id}). 2 hours ago`,
            providerOptions: { mastra: { createdAt: at.getTime() } },
          },
        ],
      },
    ],
    temperature: 0.3,
  });
  const baseline = prompt(
    new Date("2026-09-24T08:28:59.024Z"),
    "10:28 AM",
    "Sep 24 2026",
    "0f4c2f2e-4f3b-4f0e-9d7c-2a4b6c8d0e1f",
  );
  const replay = prompt(
    new Date("2026-09-25T17:02:11.070Z"),
    "7:02 PM",
    "Friday, Sep 25 2026",
    "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
  );
  expect(getOMInputFingerprint(replay)).toBe(getOMInputFingerprint(baseline));
  expect(getOMInputFingerprint({ ...replay, temperature: 0.7 })).not.toBe(
    getOMInputFingerprint(baseline),
  );
});

it("fingerprints an attachment as the reference replay history holds", () => {
  const bytes = new Uint8Array([37, 80, 68, 70]);
  const reference = fileReference({ bytes, mediaType: "application/pdf" });
  const declared =
    "https://files.example.com/v0/b/app/o/uploads%2Fquote.pdf?alt=media&token=fixture";
  const call = (label: string, data: unknown) => ({
    prompt: [
      {
        role: "user",
        content: [
          { type: "text", text: `User: see [File #1: ${label}]` },
          { type: "file", data, mediaType: "application/pdf" },
        ],
      },
    ],
  });
  const mapString = (value: string) => (value === declared ? reference : value);
  const replay = getOMInputFingerprint(
    call(reference.slice("kitaru-file://sha256/".length), new URL(reference)),
    mapString,
  );
  // Production read the URL, or Mastra downloaded it into bytes first.
  expect(
    getOMInputFingerprint(
      call("uploads/quote.pdf", new URL(declared)),
      mapString,
    ),
  ).toBe(replay);
  expect(
    getOMInputFingerprint(call("uploads/quote.pdf", bytes), mapString),
  ).toBe(replay);
  expect(
    getOMInputFingerprint(
      call("uploads/quote.pdf", new Uint8Array([1])),
      mapString,
    ),
  ).not.toBe(replay);
});

it("lets Mastra hand a replayed OM model its file URLs unread", async () => {
  const replay = createOMResultTape([], () => {});
  const observer = replay.instrument(
    { ...model(), supportedUrls: { "image/*": [/^https:\/\//] } },
    "observer",
  ) as unknown as { supportedUrls: Promise<Record<string, RegExp[]>> };
  const supported = await observer.supportedUrls;
  expect(supported["image/*"]).toEqual([/^https:\/\//]);
  // A redacted history URL the baseline never resolved must not be
  // downloaded either: the tape answers without it.
  for (const url of [
    `kitaru-file://sha256/${"0".repeat(64)}`,
    "https://files.example.com/o/quote.pdf?alt=media&token=REDACTED",
  ])
    expect(supported["*/*"]?.some((pattern) => pattern.test(url))).toBe(true);
});

it("fingerprints a recorded OM input once the turn has captured its files", async () => {
  const bytes = new Uint8Array([37, 80, 68, 70]);
  const reference = fileReference({ bytes, mediaType: "application/pdf" });
  const url = "https://files.example.com/o/quote.pdf?alt=media&token=fixture";
  const captured = new Map<string, string>();
  const tape = createOMResultTape(undefined, () => {}, {
    mapString: (value) => captured.get(value) ?? value,
    getCapturedFiles: () => new Set(captured.values()),
  });
  const call = (data: unknown) => ({
    prompt: [
      {
        role: "user",
        content: [{ type: "file", data, mediaType: "application/pdf" }],
      },
    ],
  });
  const observer = tape.instrument(model(), "observer");
  await collect((await observer.doStream(call(new URL(url)))).stream);
  // The turn's processor resolves the history URL after the observation.
  captured.set(url, reference);
  const { entries } = await tape.finish();
  expect(entries[0]?.inputFingerprint).toBe(
    getOMInputFingerprint(call(new URL(reference))),
  );
});

it("refuses a recording whose OM call read file content the turn never captured", async () => {
  const tape = createOMResultTape(undefined, () => {}, {
    getCapturedFiles: () => new Set(),
  });
  const observer = tape.instrument(model(), "observer");
  const prompt = [
    {
      role: "user",
      content: [
        {
          type: "file",
          data: new Uint8Array([37, 80, 68, 70]),
          mediaType: "application/pdf",
        },
      ],
    },
  ];
  await collect((await observer.doStream({ prompt })).stream);
  await expect(tape.finish()).rejects.toMatchObject({
    reason: "file_url_undeclared",
  });
});

it("waits for an OM call that is still in flight when the tape finishes", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const reflector = answering(() => "reflected late");
  const native = reflector.doStream;
  reflector.doStream = vi.fn(async (input: unknown) => {
    await gate;
    return native(input);
  });
  const onIncomplete = vi.fn();
  const tape = createOMResultTape(undefined, onIncomplete);
  const call = tape.instrument(reflector, "reflector").doStream({});
  let finished = false;
  const done = tape.finish().then((result) => {
    finished = true;
    return result;
  });
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(finished).toBe(false);
  release();
  await collect((await call).stream);
  const { entries } = await done;
  expect(onIncomplete).not.toHaveBeenCalled();
  expect(entries).toMatchObject([{ phase: "reflector", ordinal: 0 }]);
});
