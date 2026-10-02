import type { AdapterRunState } from "@zenml-io/kitaru/adapter";
import { expect, it, vi } from "vitest";
import { createStatefulToolProcessors } from "../src/stateful-tools.js";

it("trusts a fresh native memory executor on the second replay step", async () => {
  const ledger = new Map<string, unknown>();
  const lookup = vi.fn(async () => ({ match: null }));
  const state = {
    client: { lookupToolResult: lookup },
    failure: undefined,
    replayId: "replay",
    spec: {
      tool_policy: {
        default: { type: "history", scope: "baseline", on_miss: "fail" },
        tools: {},
      },
    },
    getToolCall: (id: string) => ledger.get(id),
    setToolCall: (entry: { callId: string }) => ledger.set(entry.callId, entry),
    getHistoryOccurrence: () => 0,
    storeFailure: vi.fn(),
  } as unknown as AdapterRunState;
  const native = vi.fn(async () => ({ updated: true }));
  const { first, last } = createStatefulToolProcessors({
    tokens: new Set(["memory-first", "memory-second"]),
    getState: () => state,
    abort: vi.fn(),
    adapter: { agentId: "agent", requestedModelId: "model" },
  });
  const tool = (id: string, execute: (...args: unknown[]) => unknown) => ({
    id,
    execute,
  });
  await first.processInputStep?.({
    tools: { updateWorkingMemory: tool("memory-first", vi.fn()) },
  } as never);
  await last.processInputStep?.({
    tools: { updateWorkingMemory: tool("memory-first", vi.fn()) },
  } as never);
  const second = tool("memory-second", native);
  await first.processInputStep?.({
    tools: { updateWorkingMemory: second },
  } as never);
  const result = (await last.processInputStep?.({
    tools: { updateWorkingMemory: second },
  } as never)) as {
    tools: {
      updateWorkingMemory: {
        execute(input: unknown, context: unknown): Promise<unknown>;
      };
    };
  };
  await expect(
    result.tools.updateWorkingMemory.execute({}, { toolCallId: "second" }),
  ).resolves.toEqual({ updated: true });
  expect(native).toHaveBeenCalledOnce();
  expect(lookup).not.toHaveBeenCalled();
});
