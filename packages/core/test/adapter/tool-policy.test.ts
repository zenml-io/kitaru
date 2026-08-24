import { describe, expect, it } from "vitest";
import {
  completeToolCall,
  decideToolCall,
  failToolCall,
} from "../../src/adapter/index.js";
import type { RunState } from "../../src/adapter/run-state.js";
import { ToolPolicyError, ToolPolicyMissError } from "../../src/errors.js";
import { type replay, runState } from "./helpers.js";

function state(
  toolPolicy: Parameters<typeof replay>[0],
  lookup?: Parameters<typeof runState>[1],
): RunState {
  return runState(toolPolicy, lookup).run;
}

describe("normalized replay policy decisions", () => {
  it("returns the first matching static result", async () => {
    const run = state({
      default: {
        cases: [
          {
            match: { tenant: "a" },
            match_mode: "subset",
            result: { value: "first" },
          },
          { match: null, match_mode: "exact", result: { value: "second" } },
        ],
        on_miss: "fail",
        type: "static",
      },
      tools: {},
    });

    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { extra: true, tenant: "a" },
        toolName: "lookup",
      }),
    ).resolves.toEqual({
      output: { value: "first" },
      type: "mocked_result",
    });
  });

  it("replays a completed null result without treating it as a miss", async () => {
    const run = state(
      {
        default: { on_miss: "passthrough", scope: "baseline", type: "history" },
        tools: {},
      },
      () => ({
        match: { error: null, result: null, status: "completed" },
      }),
    );

    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).resolves.toEqual({ output: null, type: "mocked_result" });
    expect(run.getToolCall("call-1")).toMatchObject({
      mocked: true,
      outcome: "completed",
      output: null,
    });
  });

  it.each([
    ["recorded text", "tool raised an exception", "tool raised an exception"],
    ["no text", null, "Recorded tool call 'lookup' failed"],
  ] as const)("throws a recorded failure with %s", async (_name, error, message) => {
    const { client, run } = runState(
      {
        default: { on_miss: "passthrough", scope: "baseline", type: "history" },
        tools: {},
      },
      () => ({ match: { error, result: null, status: "failed" } }),
    );

    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).rejects.toEqual(
      expect.objectContaining({ message, name: "ToolPolicyError" }),
    );
    expect(run.getToolCall("call-1")).toMatchObject({
      mocked: true,
      outcome: "failed",
      policy: "history",
    });
    expect(client.lookups[0]?.occurrence).toBe(0);
    expect(
      run.getHistoryOccurrence(client.lookups[0]?.cache_key ?? "missing"),
    ).toBe(1);
  });

  it("fails closed for an unexpected recorded status", async () => {
    const run = state(
      {
        default: { on_miss: "passthrough", scope: "baseline", type: "history" },
        tools: {},
      },
      () => ({ match: { error: null, result: null, status: "in_progress" } }),
    );

    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).rejects.toThrow("unexpected status 'in_progress'");
    expect(run.failure).toBeInstanceOf(ToolPolicyError);
    expect(run.getToolCall("call-1")).toMatchObject({
      mocked: true,
      outcome: "failed",
      policy: "history",
    });
  });

  it.each([
    ["passthrough", { type: "execute" }],
    [
      "error_result",
      {
        output: { error: "No static result for tool 'lookup'" },
        type: "mocked_error",
      },
    ],
  ] as const)("handles a static %s miss", async (onMiss, expected) => {
    const run = state({
      default: { cases: [], on_miss: onMiss, type: "static" },
      tools: {},
    });
    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).resolves.toEqual(expected);
  });

  it("throws and stores fail misses", async () => {
    const run = state({
      default: { cases: [], on_miss: "fail", type: "static" },
      tools: {},
    });
    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);
    expect(run.failure).toBeInstanceOf(ToolPolicyMissError);
  });

  it.each([
    "constructor",
    "toString",
    "__proto__",
  ])("uses only own policy entries for %s", async (toolName) => {
    type ToolPolicyConfig = NonNullable<Parameters<typeof replay>[0]>;
    const tools = JSON.parse(
      `{"${toolName}":{"type":"static","cases":[{"match":null,"match_mode":"exact","result":"own"}],"on_miss":"fail"}}`,
    ) as NonNullable<ToolPolicyConfig["tools"]>;
    const ownRun = state({ default: { type: "passthrough" }, tools });
    const fallbackRun = state({
      default: { type: "passthrough" },
      tools: {},
    });
    const input = { callId: "call-1", inputs: {}, toolName };

    await expect(decideToolCall(ownRun, input)).resolves.toEqual({
      output: "own",
      type: "mocked_result",
    });
    await expect(decideToolCall(fallbackRun, input)).resolves.toEqual({
      type: "execute",
    });
  });

  it("rejects the llm policy before execution", async () => {
    const run = state({
      default: { model: "policy-model", type: "llm" },
      tools: {},
    });
    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: {},
        toolName: "lookup",
      }),
    ).rejects.toBeInstanceOf(ToolPolicyError);
  });

  it("records passthrough completion and failure", async () => {
    const run = state({ default: { type: "passthrough" }, tools: {} });
    await decideToolCall(run, {
      callId: "success",
      inputs: {},
      toolName: "lookup",
    });
    await decideToolCall(run, {
      callId: "failure",
      inputs: {},
      toolName: "lookup",
    });
    completeToolCall(run, "success", { ok: true });
    failToolCall(run, "failure", new Error("tool failed"));

    expect(run.getToolCall("success")).toMatchObject({
      outcome: "completed",
      output: { ok: true },
    });
    expect(run.getToolCall("failure")).toMatchObject({
      error: { message: "tool failed", name: "Error" },
      outcome: "failed",
    });
  });

  it.each([
    ["oversized", () => ({ body: "x".repeat(2 * 1_048_576), sent: true })],
    [
      "circular",
      () => {
        const output: Record<string, unknown> = { sent: true };
        output.self = output;
        return output;
      },
    ],
  ])("records a %s passthrough result without failing the run", (_name, build) => {
    const run = state({ default: { type: "passthrough" }, tools: {} });
    run.setToolCall({
      callId: "call-1",
      inputs: {},
      mocked: false,
      outcome: "pending",
      toolName: "sendEmail",
    });

    // The email has already been sent by the time its result is recorded,
    // so a result too large or too circular to record must not throw.
    expect(() => completeToolCall(run, "call-1", build())).not.toThrow();
    expect(run.getToolCall("call-1")?.outcome).toBe("completed");
    expect(run.failure).toBeUndefined();
  });

  const baselineHistory = (onMiss: "fail" | "passthrough") =>
    ({
      default: { on_miss: onMiss, scope: "baseline", type: "history" },
      tools: {},
    }) as const;
  const weatherDelft = { inputs: { city: "Delft" }, toolName: "weather" };

  it("replays repeated identical baseline calls in recorded order", async () => {
    const recorded = ["first", "second", "third"];
    const { client, run } = runState(baselineHistory("fail"), (request) => ({
      match: {
        error: null,
        result: { value: recorded[request.occurrence ?? 0] },
        status: "completed",
      },
    }));

    for (const [index, value] of recorded.entries()) {
      await expect(
        decideToolCall(run, { callId: `call-${index}`, ...weatherDelft }),
      ).resolves.toEqual({ output: { value }, type: "mocked_result" });
    }
    expect(client.lookups.map((lookup) => lookup.occurrence)).toEqual([
      0, 1, 2,
    ]);
  });

  it("counts baseline occurrences per cache key, not per run", async () => {
    const { client, run } = runState(baselineHistory("fail"), (request) => ({
      match: {
        error: null,
        result: { value: request.occurrence ?? 0 },
        status: "completed",
      },
    }));

    await decideToolCall(run, { callId: "call-1", ...weatherDelft });
    await decideToolCall(run, {
      callId: "call-2",
      inputs: { city: "Rotterdam" },
      toolName: "weather",
    });
    await decideToolCall(run, { callId: "call-3", ...weatherDelft });
    // A shared per-run counter would send [0, 1, 2] here.
    expect(client.lookups.map((lookup) => lookup.occurrence)).toEqual([
      0, 0, 1,
    ]);
  });

  it("keeps the baseline occurrence counter still on a miss", async () => {
    let firstCall = true;
    const { client, run } = runState(baselineHistory("passthrough"), () => {
      if (firstCall) {
        firstCall = false;
        return { match: null };
      }
      return {
        match: {
          error: null,
          result: { value: "recorded" },
          status: "completed",
        },
      };
    });

    await expect(
      decideToolCall(run, { callId: "call-1", ...weatherDelft }),
    ).resolves.toEqual({ type: "execute" });
    await expect(
      decideToolCall(run, { callId: "call-2", ...weatherDelft }),
    ).resolves.toEqual({
      output: { value: "recorded" },
      type: "mocked_result",
    });
    expect(client.lookups.map((lookup) => lookup.occurrence)).toEqual([0, 0]);
  });

  it.each([
    "agent",
    "cohort_version",
  ] as const)("sends no occurrence for the %s scope", async (scope) => {
    const { client, run } = runState(
      {
        default: { on_miss: "fail", scope, type: "history" },
        tools: {},
      },
      () => ({
        match: {
          error: null,
          result: { value: "recorded" },
          status: "completed",
        },
      }),
    );
    await decideToolCall(run, { callId: "call-1", ...weatherDelft });
    await decideToolCall(run, { callId: "call-2", ...weatherDelft });
    expect(client.lookups).toHaveLength(2);
    for (const lookup of client.lookups) {
      expect(lookup).not.toHaveProperty("occurrence");
    }
  });

  it("follows on_miss for a call past the last recorded occurrence", async () => {
    const { client, run } = runState(baselineHistory("fail"), (request) =>
      (request.occurrence ?? 0) < 1
        ? {
            match: {
              error: null,
              result: { value: "only" },
              status: "completed",
            },
          }
        : { match: null },
    );

    await expect(
      decideToolCall(run, { callId: "call-1", ...weatherDelft }),
    ).resolves.toEqual({ output: { value: "only" }, type: "mocked_result" });
    await expect(
      decideToolCall(run, { callId: "call-2", ...weatherDelft }),
    ).rejects.toBeInstanceOf(ToolPolicyMissError);
    expect(client.lookups.map((lookup) => lookup.occurrence)).toEqual([0, 1]);
  });

  it("refuses a later tool once a policy has already failed", async () => {
    const run = state({ default: { type: "passthrough" }, tools: {} });
    const failure = new ToolPolicyMissError("No static result for 'normalize'");
    run.storeFailure(failure);

    await expect(
      decideToolCall(run, {
        callId: "call-2",
        inputs: {},
        toolName: "sendEmail",
      }),
    ).rejects.toBe(failure);
    expect(run.getToolCall("call-2")).toBeUndefined();
  });
});
