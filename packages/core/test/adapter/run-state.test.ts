import { describe, expect, it } from "vitest";

import { RunState } from "../../src/adapter/run-state.js";
import { fakeClient, SESSION_ID } from "./helpers.js";

function state(): RunState {
  return new RunState({
    client: fakeClient(),
    effectiveInput: "input",
    requestedModelId: "model",
    sessionId: SESSION_ID,
  });
}

describe("adapter run state", () => {
  it("isolates indexes, ledgers, and failures across invocations", () => {
    const first = state();
    const second = state();
    first.setToolCall({
      callId: "same-call",
      inputs: { run: 1 },
      mocked: false,
      outcome: "pending",
      toolName: "lookup",
    });
    first.storeFailure(new Error("first failed"));

    expect(first.allocateNode()).toEqual({ index: 1 });
    expect(second.allocateNode()).toEqual({ index: 1 });
    expect(second.getToolCall("same-call")).toBeUndefined();
    expect(second.failure).toBeUndefined();
  });

  it("serializes queued writes in enqueue order", async () => {
    const run = state();
    const events: string[] = [];
    let releaseFirst: (() => void) | undefined;
    const first = run.enqueueStep(
      () =>
        new Promise<void>((resolve) => {
          events.push("first-start");
          releaseFirst = () => {
            events.push("first-end");
            resolve();
          };
        }),
    );
    const second = run.enqueueStep(async () => {
      events.push("second");
    });

    await Promise.resolve();
    expect(events).toEqual(["first-start"]);
    releaseFirst?.();
    await Promise.all([first, second]);
    expect(events).toEqual(["first-start", "first-end", "second"]);
  });

  it("keeps recording steps after one step fails", async () => {
    const run = state();
    const recorded: string[] = [];
    const failed = run
      .enqueueStep(async () => {
        throw new Error("upsert failed");
      })
      .catch((error: unknown) => error);
    const next = run.enqueueStep(async () => {
      recorded.push("second");
    });

    await expect(failed).resolves.toMatchObject({ message: "upsert failed" });
    await expect(next).resolves.toBeUndefined();
    expect(recorded).toEqual(["second"]);
    await expect(run.awaitSteps()).rejects.toThrow("upsert failed");
  });

  it("reports the earliest known start for each step", () => {
    const run = state();
    const first = run.takeStepStart("2026-01-01T00:00:05.000Z");
    expect(Date.parse(first)).toBeLessThanOrEqual(
      Date.parse("2026-01-01T00:00:05.000Z"),
    );
    expect(run.takeStepStart("2026-01-01T00:00:09.000Z")).toBe(
      "2026-01-01T00:00:05.000Z",
    );
  });

  it("keeps the first stored failure", () => {
    const run = state();
    const first = new Error("first");
    run.storeFailure(first);
    run.storeFailure(new Error("second"));
    expect(run.failure).toBe(first);
  });
});
