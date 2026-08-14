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

  it.each([
    "fail",
    "error_result",
    "passthrough",
  ] as const)("fails closed for an ambiguous null history result with %s on_miss", async (onMiss) => {
    const run = state(
      {
        default: { on_miss: onMiss, scope: "baseline", type: "history" },
        tools: {},
      },
      () => ({ found: true, result: null }),
    );

    await expect(
      decideToolCall(run, {
        callId: "call-1",
        inputs: { value: 1 },
        toolName: "lookup",
      }),
    ).rejects.toThrow(
      /cannot distinguish a failed recording from a null result/,
    );
    expect(run.failure).toBeInstanceOf(ToolPolicyError);
    expect(run.getToolCall("call-1")).toMatchObject({
      mocked: false,
      outcome: "failed",
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
