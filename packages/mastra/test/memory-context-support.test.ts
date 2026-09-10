import { RequestContext } from "@mastra/core/request-context";
import { describe, expect, it } from "vitest";
import { canCaptureMemoryContext } from "../src/memory-context-support.js";

describe("memory snapshot support", () => {
  it("accepts ordinary history with a read-only or bounded recall configuration", () => {
    const context = new RequestContext();
    context.set("MastraMemory", {
      memoryConfig: { lastMessages: 5, readOnly: true },
    });
    expect(canCaptureMemoryContext(context)).toBe(true);
  });

  it.each([
    { workingMemory: { enabled: true } },
    { semanticRecall: true },
    { semanticRecall: { topK: 3, messageRange: 2 } },
    { observationalMemory: true },
    { observationalMemory: { scope: "thread" } },
  ])("refuses memory behavior a fixed snapshot cannot replay: %j", (memoryConfig) => {
    const context = new RequestContext();
    context.set("MastraMemory", { memoryConfig });
    expect(canCaptureMemoryContext(context)).toBe(false);
  });

  it("does not treat disabled memory features as missing context", () => {
    const context = new RequestContext();
    context.set("MastraMemory", {
      memoryConfig: {
        workingMemory: { enabled: false },
        semanticRecall: false,
        observationalMemory: false,
      },
    });
    expect(canCaptureMemoryContext(context)).toBe(true);
  });
});
