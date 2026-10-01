import type { Agent } from "@mastra/core/agent";
import { InMemoryStore } from "@mastra/core/storage";
import { Memory } from "@mastra/memory";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  type MemoryReplayAgentOptions,
} from "@zenml-io/kitaru-mastra/memory";
import { z } from "zod";

const store = new InMemoryStore();
const sourceMemory = new Memory({
  storage: store,
  options: {
    semanticRecall: false,
    workingMemory: {
      enabled: true,
      scope: "thread",
      schema: z.object({ preference: z.string() }),
    },
  },
});
const exclusiveAccess = createProcessLocalMemoryAccess();
const options: MemoryReplayAgentOptions = {
  agentId: "018f0000-0000-7000-8000-000000000103",
  requestedModelId: "openai/gpt-5-mini",
  allowedReplayModels: ["openai/gpt-5-mini"],
  sourceMemory: () => ({
    domain: store.stores.memory!,
    configuration: sourceMemory.getMergedThreadConfig(),
    settled: () => sourceMemory.settled(),
    exclusiveAccess,
  }),
  resolveModel: (id) => {
    if (id !== "openai/gpt-5-mini") throw new Error(`Unknown model: ${id}`);
    return "openai/gpt-5-mini";
  },
};
const recorded = createMemoryReplayAgent(
  ({ memory }) => ({
    id: "support",
    name: "Support",
    memory,
    instructions: () => "Remember the user's preferences.",
    model: () => "openai/gpt-5-mini",
    defaultOptions: () => ({ maxSteps: 3 }),
  }),
  options,
);
const nativeStream: Agent["stream"] = recorded.stream;
void nativeStream;
