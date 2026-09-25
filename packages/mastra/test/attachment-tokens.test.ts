import type { Memory } from "@mastra/memory";
import { expect, it, vi } from "vitest";
import {
  recordAttachmentTokens,
  replayAttachmentTokens,
} from "../src/attachment-tokens.js";
import { fileReference } from "../src/stateful-files.js";

type OMEngine = NonNullable<Awaited<Memory["omEngine"]>>;

/** An OM engine whose token counter answers with fixed counts. */
function engineCounting(sync: number, provider: number) {
  const counter = {
    countAttachmentPartSync: vi.fn((_part: unknown) => sync),
    countAttachmentPartAsync: vi.fn(async (_part: unknown) => provider),
  };
  const engine = { getTokenCounter: () => counter } as unknown as OMEngine;
  return { counter, engine };
}

const BYTES = new Uint8Array([37, 80, 68, 70, 45, 55]);

it.each([
  ["base64 text", Buffer.from(BYTES).toString("base64")],
  [
    "a data URL",
    `data:application/pdf;base64,${Buffer.from(BYTES).toString("base64")}`,
  ],
  ["bytes", BYTES],
])(
  "replays the count of an attachment a processor held inline as %s",
  async (_, data) => {
    const part = { type: "file", data, mimeType: "application/pdf" };
    const baseline = engineCounting(40, 1_200);
    const recording = recordAttachmentTokens(baseline.engine, (key) => key);
    expect(await baseline.counter.countAttachmentPartAsync(part)).toBe(1_200);
    const reference = fileReference({
      bytes: BYTES,
      mediaType: "application/pdf",
    });
    const counts = recording.counts();
    expect(counts).toEqual({ [reference]: { async: 1_200 } });

    // Replay's processor inlines the recorded bytes again; the provider
    // must not be asked to count them.
    const replay = engineCounting(40, 9_999);
    const providerCount = replay.counter.countAttachmentPartAsync;
    replayAttachmentTokens(replay.engine, counts);
    expect(
      await replay.counter.countAttachmentPartAsync({
        type: "file",
        data: Buffer.from(BYTES).toString("base64"),
        mimeType: "application/pdf",
      }),
    ).toBe(1_200);
    // Inline content the baseline never counted takes Mastra's local estimate.
    expect(
      await replay.counter.countAttachmentPartAsync({
        type: "file",
        data: Buffer.from([1, 2, 3]).toString("base64"),
        mimeType: "application/pdf",
      }),
    ).toBe(40);
    expect(providerCount).not.toHaveBeenCalled();
  },
);
