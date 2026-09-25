import { createHash } from "node:crypto";
import { expect, it } from "vitest";
import {
  createContextInput,
  restoreConversationContext,
} from "../src/conversation-context.js";
import {
  createMemoryReplayEnvelope,
  decodeMemoryReplayEnvelope,
  decodeMemoryValue,
  encodeMemoryValue,
  finalizeMemoryReplayEnvelope,
} from "../src/memory-snapshot.js";
import { createRecordedEvidenceSanitizer } from "../src/stateful-files.js";
import {
  createMemoryRuntime,
  FILE_BYTES,
  seedMemory,
  snapshotMemory,
} from "./helpers/memory-agent.js";

const FILE_REF = `kitaru-file://sha256/${createHash("sha256").update("application/pdf\0").update(FILE_BYTES).digest("hex")}`;
const FILE_BLOB_ID = "018f0000-0000-7000-8002-000000000400";
const FILE_SHA256 = createHash("sha256").update(FILE_BYTES).digest("hex");

/** A file entry recorded before blob storage, with its bytes inline. */
function inlineFileEntry(patch: Record<string, unknown> = {}) {
  return {
    url: FILE_REF,
    mediaType: "application/pdf",
    base64: Buffer.from(FILE_BYTES).toString("base64"),
    length: FILE_BYTES.byteLength,
    sha256: FILE_SHA256,
    ...patch,
  };
}

function required<T>(value: T | undefined | null): T {
  if (value === undefined || value === null)
    throw new Error("Missing fixture value");
  return value;
}

async function fixture() {
  const runtime = createMemoryRuntime();
  await seedMemory(runtime);
  return {
    invocationId: "invocation-1",
    rawInput: [
      { role: "user", content: [{ type: "file", data: new URL(FILE_REF) }] },
    ],
    initialSnapshot: {
      ...(await snapshotMemory(runtime, true)),
      threadId: "historical-thread",
      resourceId: "historical-resource",
    },
    configuration: {
      instructions: "Answer",
      model: { provider: "fixture", modelId: "actor" },
      memory: {
        semanticRecall: false,
        workingMemory: { scope: "thread", schema: { type: "object" } },
        observationalMemory: { scope: "thread" },
      },
    },
    requestContext: { locale: "en" },
    files: [
      {
        url: FILE_REF,
        mediaType: "application/pdf",
        blobId: FILE_BLOB_ID,
        length: FILE_BYTES.byteLength,
        sha256: FILE_SHA256,
      },
    ],
    omTape: [],
    turnStartedAt: new Date("2026-09-01T09:00:00.000Z"),
  };
}

it("round-trips historical memory Dates, undefined fields, URL input and lossless binary", async () => {
  const input = await fixture();
  required(input.initialSnapshot.records[0]).lastBufferedAtTime = new Date(123);
  required(input.initialSnapshot.records[0]).bufferedReflection =
    "pending reflection";
  required(input.initialSnapshot.records[0]).bufferedMessageIds = [
    "historical-message",
  ];
  const envelope = createMemoryReplayEnvelope(input);
  expect(envelope.complete).toBe(true);
  const restored = decodeMemoryReplayEnvelope(
    JSON.parse(JSON.stringify(envelope)),
  );
  expect(restored).toEqual(input);
  expect(restored.initialSnapshot.messages[0]?.createdAt).toBeInstanceOf(Date);
  expect(decodeMemoryValue(encodeMemoryValue(FILE_BYTES))).toEqual(FILE_BYTES);
});

it.each(["version", "hash", "missing", "inflight"])(
  "rejects invalid replay prerequisites: %s",
  async (kind) => {
    const envelope = createMemoryReplayEnvelope(await fixture());
    if (kind === "version") envelope.version = 4 as 3;
    if (kind === "hash") required(envelope.files[0]).sha256 = "0".repeat(64);
    if (kind === "missing") envelope.initialSnapshot = {};
    if (kind === "inflight") {
      const input = await fixture();
      required(input.initialSnapshot.records[0]).isObserving = true;
      Object.assign(envelope, createMemoryReplayEnvelope(input));
    }
    expect(() => decodeMemoryReplayEnvelope(envelope)).toThrow(
      /Unsupported Mastra memory replay/,
    );
  },
);

it.each([
  { apiKey: "private-value" },
  { callback: () => "live" },
  { value: "a".repeat(16_777_216) },
  { value: new Map([["key", "value"]]) },
  { value: Array.from({ length: 200_001 }, () => 1) },
])(
  "marks altered or oversized state incomplete without exposing credentials",
  async (configuration) => {
    const envelope = createMemoryReplayEnvelope({
      ...(await fixture()),
      configuration,
    });
    expect(envelope.complete).toBe(false);
    expect(JSON.stringify(envelope)).not.toContain("private-value");
    expect(() => decodeMemoryReplayEnvelope(envelope)).toThrow();
  },
);

it("rejects reserved codec tags and cyclic objects instead of accepting ambiguous data", () => {
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  expect(() => encodeMemoryValue(cycle)).toThrow();
  expect(() =>
    encodeMemoryValue({ $mastra: "date", value: "2026-01-01" }),
  ).toThrow();
});

it("keeps the version-1 recalled conversation contract", () => {
  const messages = [{ role: "user", content: "hello" }];
  expect(
    restoreConversationContext(createContextInput(messages, messages)),
  ).toEqual(messages);
});

it("round-trips complete buffered chunks and every historical OM generation", async () => {
  const input = await fixture();
  required(input.initialSnapshot.records[0]).bufferedObservationChunks = [
    {
      id: "chunk-1",
      cycleId: "cycle-1",
      observations: "buffered",
      tokenCount: 3,
      messageIds: ["historical-message"],
      messageTokens: 7,
      lastObservedAt: new Date(100),
      createdAt: new Date(110),
      extractedValues: { next: "wait" },
    },
  ];
  input.initialSnapshot.records.push({
    ...required(input.initialSnapshot.records[0]),
    id: "older-generation",
    generationCount: 1,
    originType: "reflection",
  });
  expect(decodeMemoryReplayEnvelope(createMemoryReplayEnvelope(input))).toEqual(
    input,
  );
});

it("round-trips an empty initial conversation", async () => {
  const input = await fixture();
  input.initialSnapshot = {
    threadId: "new",
    resourceId: "new-resource",
    thread: null,
    resource: null,
    messages: [],
    records: [],
  };
  expect(
    decodeMemoryReplayEnvelope(createMemoryReplayEnvelope(input))
      .initialSnapshot,
  ).toEqual(input.initialSnapshot);
});

it("counts aggregate envelope items, depth, and binary expansion against the shared budget", async () => {
  const input = await fixture();
  const tooMany = createMemoryReplayEnvelope({
    ...input,
    configuration: { values: Array.from({ length: 110_000 }, () => 1) },
    requestContext: { values: Array.from({ length: 110_000 }, () => 1) },
  });
  expect(tooMany.complete).toBe(false);
  expect(tooMany.reasons[0]).toMatch(/maximum item count 200000/);
  const deep = Array.from({ length: 65 }).reduce<unknown>(
    (value) => ({ value }),
    null,
  );
  expect(() => encodeMemoryValue(deep)).toThrow(/depth/);
  const oversized = await fixture();
  required(oversized.files[0]).length = 16 * 1_048_576 + 1;
  expect(createMemoryReplayEnvelope(oversized).complete).toBe(false);
});

it("keeps stored file bytes out of the envelope and its JSON budget", async () => {
  const input = await fixture();
  input.initialSnapshot.messages = [
    {
      ...required(input.initialSnapshot.messages[0]),
      content: {
        format: 2,
        parts: [{ type: "text", text: "x".repeat(10 * 1_048_576) }],
      },
    },
  ];
  const bytes = new Uint8Array(9 * 1_048_576).fill(7);
  const url = `kitaru-file://sha256/${createHash("sha256").update("application/pdf\0").update(bytes).digest("hex")}`;
  const envelope = createMemoryReplayEnvelope({
    ...input,
    files: [{ url, mediaType: "application/pdf", bytes, blobId: FILE_BLOB_ID }],
  });
  expect(envelope.complete, envelope.reasons.join("; ")).toBe(true);
  expect(envelope.files).toEqual([
    {
      url,
      mediaType: "application/pdf",
      blobId: FILE_BLOB_ID,
      length: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    },
  ]);
  expect(JSON.stringify(envelope).length).toBeLessThan(11 * 1_048_576);
});

it("reads envelopes recorded with inline file bytes", async () => {
  const input = await fixture();
  const envelope = finalizeMemoryReplayEnvelope(
    createMemoryReplayEnvelope(input),
    [],
    (value) => ({
      ...(value as Record<string, unknown>),
      files: [inlineFileEntry()],
    }),
  );
  expect(decodeMemoryReplayEnvelope(envelope).files).toEqual([
    { url: FILE_REF, mediaType: "application/pdf", bytes: FILE_BYTES },
  ]);
});

it.each([50, 830])(
  "round-trips %i-message history with 15k nested fields and a file",
  async (count) => {
    const input = await fixture();
    const original = required(input.initialSnapshot.messages[0]);
    input.initialSnapshot.messages = Array.from(
      { length: count },
      (_, index) => ({
        ...original,
        id: `message-${index}`,
        content: {
          ...original.content,
          parts: [
            {
              type: "text",
              text: index === 0 ? "x".repeat(1_100_000) : `message ${index}`,
            },
          ],
          metadata:
            index === 0
              ? {
                  hotels: Array.from({ length: 1_500 }, (_, hotel) => ({
                    id: hotel,
                    details: Object.fromEntries(
                      Array.from({ length: 10 }, (_, field) => [
                        `field${field}`,
                        field,
                      ]),
                    ),
                  })),
                }
              : undefined,
        },
      }),
    );
    const envelope = createMemoryReplayEnvelope(input);
    expect(envelope.complete).toBe(true);
    expect(JSON.stringify(envelope).length).toBeGreaterThan(1_048_576);
    expect(
      decodeMemoryReplayEnvelope(JSON.parse(JSON.stringify(envelope))),
    ).toEqual(input);
  },
);

it("finalizes a separate version-3 envelope with an ordered OM tape", async () => {
  const provisional = createMemoryReplayEnvelope(await fixture());
  const final = finalizeMemoryReplayEnvelope(provisional, [
    { phase: "observation", ordinal: 0, output: "remember" },
  ]);
  expect(final).not.toBe(provisional);
  expect(provisional.omTape).toEqual([]);
  expect(decodeMemoryReplayEnvelope(final).omTape).toEqual([
    { phase: "observation", ordinal: 0, output: "remember" },
  ]);
  expect(() =>
    finalizeMemoryReplayEnvelope(provisional, [{ token: "secret" }]),
  ).toThrow();
});

it("keeps recorded attachment token counts and refuses malformed ones", async () => {
  const provisional = createMemoryReplayEnvelope(await fixture());
  const reference = `kitaru-file://sha256/${"a".repeat(64)}`;
  const counts = { [reference]: { sync: 20, async: 1_500 } };
  const final = finalizeMemoryReplayEnvelope(
    provisional,
    [],
    undefined,
    counts,
  );
  expect(decodeMemoryReplayEnvelope(final).attachmentTokens).toEqual(counts);
  expect(
    decodeMemoryReplayEnvelope(finalizeMemoryReplayEnvelope(provisional, []))
      .attachmentTokens,
  ).toBeUndefined();
  expect(() =>
    finalizeMemoryReplayEnvelope(provisional, [], undefined, {
      [reference]: { sync: -1 },
    }),
  ).toThrow(/attachment token counts/);
  // A history file the turn never resolved keeps its redacted URL as key.
  const redacted = {
    "https://files.invalid/a.pdf?alt=media&token=REDACTED": { sync: 1 },
  };
  expect(
    decodeMemoryReplayEnvelope(
      finalizeMemoryReplayEnvelope(provisional, [], undefined, redacted),
    ).attachmentTokens,
  ).toEqual(redacted);
  expect(() =>
    finalizeMemoryReplayEnvelope(provisional, [], undefined, {
      "files.invalid/a.pdf": { sync: 1 },
    }),
  ).toThrow(/attachment token counts/);
});

it("normalizes implicit thread OM and rejects old OM envelopes without a tape", async () => {
  const input = await fixture();
  delete (input.configuration.memory.observationalMemory as { scope?: string })
    .scope;
  const current = createMemoryReplayEnvelope(input);
  expect(current.complete).toBe(true);
  expect(
    decodeMemoryReplayEnvelope(current).configuration.memory,
  ).toMatchObject({
    observationalMemory: { scope: "thread" },
  });
  const old = { ...current, version: 2 };
  delete (old as { omTape?: unknown }).omTape;
  expect(() => decodeMemoryReplayEnvelope(old)).toThrow(
    /mastra_om_tape_missing/,
  );
  const splitConfig = {
    ...old,
    configuration: encodeMemoryValue({
      memoryConfig: {},
      memory: { observationalMemory: { scope: "thread" } },
    }),
  };
  expect(() => decodeMemoryReplayEnvelope(splitConfig)).toThrow(
    /mastra_om_tape_missing/,
  );
  const workingOnly = await fixture();
  delete (workingOnly.configuration.memory as { observationalMemory?: unknown })
    .observationalMemory;
  // Version-2 envelopes hold file bytes inline.
  const oldWorking = {
    ...createMemoryReplayEnvelope(workingOnly),
    version: 2,
    files: [inlineFileEntry()],
  };
  delete (oldWorking as { omTape?: unknown }).omTape;
  expect(decodeMemoryReplayEnvelope(oldWorking).omTape).toBeUndefined();
});

it.each(["resourceScope", "semanticRecall", "buffer", "ids", "date", "url"])(
  "rejects malformed or out-of-scope state: %s",
  async (kind) => {
    const input = await fixture();
    if (kind === "resourceScope")
      required(input.initialSnapshot.records[0]).scope = "resource";
    if (kind === "semanticRecall")
      input.configuration.memory.semanticRecall = true;
    if (kind === "buffer")
      required(input.initialSnapshot.records[0]).bufferedObservationChunks = [
        {} as never,
      ];
    if (kind === "ids")
      required(input.initialSnapshot.messages[0]).threadId = "different-thread";
    if (kind === "date")
      required(input.initialSnapshot.messages[0]).createdAt = new Date(
        Number.NaN,
      );
    if (kind === "url")
      required(required(input.rawInput[0]).content[0]).data = new URL(
        "ftp://private-value@files.invalid/file",
      );
    const envelope = createMemoryReplayEnvelope(input);
    expect(envelope.complete).toBe(false);
    expect(JSON.stringify(envelope)).not.toContain("private-value");
  },
);

it("redacts URL credentials in replay input and history instead of refusing them", async () => {
  const input = await fixture();
  required(required(input.rawInput[0]).content[0]).data = new URL(
    "https://files.invalid/file?apiKey=private-value",
  );
  const message = required(input.initialSnapshot.messages[0]);
  message.content.parts = [
    {
      type: "text",
      text: "Saved https://firebasestorage.googleapis.com/v0/b/b/o/a.pdf?alt=media&token=private-token",
    },
  ];
  const envelope = createMemoryReplayEnvelope(input);
  expect(envelope.complete, envelope.reasons.join("; ")).toBe(true);
  expect(JSON.stringify(envelope)).not.toMatch(/private-value|private-token/);
  const decoded = decodeMemoryReplayEnvelope(envelope);
  expect(
    String(
      required(required(decoded.rawInput as typeof input.rawInput)[0])
        .content[0]?.data,
    ),
  ).toBe("https://files.invalid/file?apiKey=REDACTED");
  expect(JSON.stringify(decoded.initialSnapshot.messages[0])).toContain(
    "alt=media&token=REDACTED",
  );
});

it("rejects changed file lengths, noncanonical base64, and malformed date tags", async () => {
  const input = await fixture();
  for (const patch of [{ length: 100 }, { base64: "???" }]) {
    const envelope = createMemoryReplayEnvelope(input);
    envelope.files = [inlineFileEntry(patch)];
    expect(() => decodeMemoryReplayEnvelope(envelope)).toThrow(/binary/);
  }
  expect(() =>
    decodeMemoryValue({ $mastra: "date", value: "2026-01-01" }),
  ).toThrow(/Date/);
});

/** Rebuild objects in PostgreSQL `jsonb` key order: shorter keys first, then bytes. */
function jsonbOrder(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(jsonbOrder);
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(
        ([left], [right]) =>
          Buffer.byteLength(left) - Buffer.byteLength(right) ||
          Buffer.compare(Buffer.from(left), Buffer.from(right)),
      )
      .map(([key, item]) => [key, jsonbOrder(item)]),
  );
}

it("restores recorded key order after storage re-sorts object keys", async () => {
  const input = await fixture();
  input.configuration.memory.workingMemory.schema = {
    type: "object",
    properties: {
      favoriteColor: { type: "string" },
      name: { type: "string" },
      city: { type: "string" },
    },
    required: ["favoriteColor"],
  } as never;
  required(input.initialSnapshot.messages[0]).content.parts = [
    {
      type: "tool-invocation",
      toolInvocation: {
        state: "result",
        toolCallId: "call-1",
        toolName: "lookupOrder",
        args: { orderNumber: "A-1", zip: "2611" },
        result: { status: "shipped", eta: "tomorrow" },
      },
    },
  ];
  const envelope = createMemoryReplayEnvelope(input);
  expect(envelope.complete, envelope.reasons.join("; ")).toBe(true);
  const stored = jsonbOrder(JSON.parse(JSON.stringify(envelope)));
  expect(JSON.stringify(stored)).not.toBe(JSON.stringify(envelope));
  const decoded = decodeMemoryReplayEnvelope(stored);
  expect(JSON.stringify(decoded.configuration)).toBe(
    JSON.stringify(input.configuration),
  );
  expect(JSON.stringify(decoded.initialSnapshot.messages)).toBe(
    JSON.stringify(input.initialSnapshot.messages),
  );
  expect(decoded).toEqual(input);
});

it("refuses envelopes whose key order is missing or does not restore them", async () => {
  const envelope = createMemoryReplayEnvelope(await fixture());
  const { keyOrder, ...withoutOrder } = envelope;
  expect(() => decodeMemoryReplayEnvelope(withoutOrder)).toThrow(
    /Missing recorded key order/,
  );
  for (const permutations of ["x", "0:0,0", "0:5,1,0"])
    expect(() =>
      decodeMemoryReplayEnvelope({
        ...envelope,
        keyOrder: { ...keyOrder, permutations },
      }),
    ).toThrow(/key order/);
  expect(() =>
    decodeMemoryReplayEnvelope({ ...envelope, invocationId: "invocation-2" }),
  ).toThrow(/differs from the recorded envelope/);
  const withoutTime = { ...envelope, turnStartedAt: "yesterday" };
  expect(() => decodeMemoryReplayEnvelope(withoutTime)).toThrow(
    /turn start time/,
  );
});

it("records key order after the upload sanitizer rewrites declared file URLs", async () => {
  const input = {
    ...(await fixture()),
    requestContext: { attachment: "https://files.invalid/quote.pdf" },
  };
  const sanitizer = createRecordedEvidenceSanitizer(
    new Map([["https://files.invalid/quote.pdf", FILE_REF]]),
    () => undefined,
  );
  const provisional = createMemoryReplayEnvelope(input, sanitizer.replace);
  expect(provisional.complete, provisional.reasons.join("; ")).toBe(true);
  const final = finalizeMemoryReplayEnvelope(
    provisional,
    [{ output: "https://files.invalid/quote.pdf" }],
    sanitizer.replace,
  );
  for (const envelope of [provisional, final]) {
    const uploaded = sanitizer.replace(envelope);
    expect(decodeMemoryReplayEnvelope(uploaded).requestContext).toEqual({
      attachment: FILE_REF,
    });
  }
  expect(decodeMemoryReplayEnvelope(final).omTape).toEqual([
    { output: FILE_REF },
  ]);
});
