import { describe, expect, it, vi } from "vitest";
import {
  DEFAULT_PROMPT,
  DEFAULT_SYSTEM,
  LIMITS,
  type Message,
  MODEL,
  runConversation,
  type Transcript,
} from "../src/conversation.js";
import { resolveTarget } from "../src/main.js";

const target = { model: MODEL, system: DEFAULT_SYSTEM, maxOutputTokens: 2000 };
const checkpoint = async () => {};

describe("adaptive fixture conversation", () => {
  it("branches from responses and passes fresh explicit complete history", async () => {
    const histories: Message[][] = [];
    const createTarget = vi.fn(() => async (history: Message[]) => {
      histories.push(history);
      return {
        text:
          history.length === 1
            ? "What is your order reference?"
            : "FIXTURE-42 is delayed. Please contact support.",
        totalTokens: 10,
      };
    });
    for (let repeat = 0; repeat < 2; repeat++) {
      const result = await runConversation({
        prompt: DEFAULT_PROMPT,
        target,
        createTarget,
        checkpoint,
      });
      expect(result.stop_reason).toBe("scenario_complete");
      expect(result.branches).toEqual(["provide_reference"]);
      expect(result.messages).toHaveLength(4);
      expect(result.total_tokens).toBe(20);
    }
    expect(createTarget).toHaveBeenCalledTimes(2);
    expect(histories.map((history) => history.length)).toEqual([1, 3, 1, 3]);
    expect(histories[0]).not.toBe(histories[2]);
  });

  it("uses a different branch for an unhelpful response and stops at turn bound", async () => {
    const result = await runConversation({
      prompt: DEFAULT_PROMPT,
      target,
      checkpoint,
      createTarget: () => async () => ({ text: "Hello", totalTokens: 1 }),
    });
    expect(result.stop_reason).toBe("turn_limit");
    expect(result.branches).toEqual([
      "request_clarification",
      "request_clarification",
    ]);
    expect(result.messages).toHaveLength(6);
  });

  it("keeps target variants separate from fixed simulator and evaluator provenance", async () => {
    const variant = resolveTarget({
      system_prompt: "Different target instructions",
      model_params: { maxOutputTokens: 100 },
    });
    const createTarget = vi.fn(() => async () => ({
      text: "FIXTURE-42 is delayed",
      totalTokens: 2,
    }));
    const result = await runConversation({
      prompt: "Different target initial prompt",
      target: variant,
      checkpoint,
      createTarget,
    });
    expect(createTarget).toHaveBeenCalledWith(variant);
    expect(result.messages[0]?.content).toBe("Different target initial prompt");
    expect(result.simulator).toEqual({
      kind: "deterministic",
      version: "parcel-fixture-v1",
    });
    expect(result.judge).toEqual({
      kind: "python",
      configuration: "independent-evaluator-job",
    });
    expect(() =>
      resolveTarget({ model_params: { maxOutputTokens: 2001 } }),
    ).toThrow();
    expect(() => resolveTarget({ model_params: { temperature: 1 } })).toThrow();
  });

  it("stops on reported usage before making another call", async () => {
    const generate = vi.fn(async () => ({
      text: "What is your order?",
      totalTokens: LIMITS.totalTokens,
    }));
    const result = await runConversation({
      prompt: DEFAULT_PROMPT,
      target,
      checkpoint,
      createTarget: () => generate,
    });
    expect(result.stop_reason).toBe("usage_limit");
    expect(generate).toHaveBeenCalledTimes(1);
  });

  it("fails a hung provider call and records the unanswered user turn", async () => {
    let signal: AbortSignal | undefined;
    const snapshots: Transcript[] = [];
    await expect(
      runConversation({
        prompt: DEFAULT_PROMPT,
        target,
        checkpoint: async (value) => {
          snapshots.push(value);
        },
        limits: { ...LIMITS, callMs: 5 },
        createTarget: () => async (_, abort) => {
          signal = abort;
          return new Promise(() => {});
        },
      }),
    ).rejects.toThrow();
    expect(signal?.aborted).toBe(true);
    expect(snapshots.at(-1)?.stop_reason).toBe("call_timeout");
    expect(snapshots.at(-1)?.messages).toEqual([
      { role: "user", content: DEFAULT_PROMPT },
    ]);
  });

  it("completes at the overall elapsed bound without starting a call after slow checkpoint", async () => {
    const generate = vi.fn(async () => ({ text: "unused", totalTokens: 1 }));
    const result = await runConversation({
      prompt: DEFAULT_PROMPT,
      target,
      checkpoint: async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
      },
      limits: { ...LIMITS, elapsedMs: 1 },
      createTarget: () => generate,
    });
    expect(result.stop_reason).toBe("time_limit");
    expect(generate).not.toHaveBeenCalled();
  });

  it("keeps the overall deadline classification when the provider rejects on abort", async () => {
    let aborted = false;
    const result = await runConversation({
      prompt: DEFAULT_PROMPT,
      target,
      checkpoint,
      limits: { ...LIMITS, elapsedMs: 10 },
      createTarget: () => async (_, signal) =>
        new Promise((_, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              aborted = true;
              reject(new DOMException("Aborted", "AbortError"));
            },
            { once: true },
          );
        }),
    });
    expect(aborted).toBe(true);
    expect(result.stop_reason).toBe("time_limit");
    expect(result.stop_detail).toBe("conversation_deadline");
  });

  it("checkpoints partial failure without leaking provider errors", async () => {
    const snapshots: Transcript[] = [];
    await expect(
      runConversation({
        prompt: DEFAULT_PROMPT,
        target,
        checkpoint: async (value) => {
          snapshots.push(value);
        },
        createTarget: () => async () => {
          throw new Error("secret provider detail");
        },
      }),
    ).rejects.toThrow("Conversation target or checkpoint failed");
    expect(snapshots.at(-1)?.stop_reason).toBe("runtime_failure");
    expect(snapshots.at(-1)?.messages).toHaveLength(1);
    expect(JSON.stringify(snapshots)).not.toContain("secret");
  });

  it.each([
    { text: "", totalTokens: 2 },
    { text: "x".repeat(LIMITS.textChars + 1), totalTokens: 2 },
    { text: "answer", totalTokens: Number.NaN },
  ])("rejects invalid or unbounded output", async (reply) => {
    await expect(
      runConversation({
        prompt: DEFAULT_PROMPT,
        target,
        checkpoint,
        createTarget: () => async () => reply,
      }),
    ).rejects.toThrow();
  });
});
