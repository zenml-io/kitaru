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

  it("keeps the first stored failure", () => {
    const run = state();
    const first = new Error("first");
    run.storeFailure(first);
    run.storeFailure(new Error("second"));
    expect(run.failure).toBe(first);
  });
});
