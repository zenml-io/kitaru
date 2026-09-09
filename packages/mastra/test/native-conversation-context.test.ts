import { Agent } from "@mastra/core/agent";
import { MastraMemory } from "@mastra/core/memory";
import { InMemoryStore } from "@mastra/core/storage";
// @ts-expect-error Mastra 1.51.0 exports this public test helper without declarations.
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

// Keep Mastra's native memory processors and storage, implementing only the
// abstract persistence interface that its base class leaves to integrations.
class HistoryMemory extends MastraMemory {
  readonly domain;

  constructor() {
    const storage = new InMemoryStore();
    super({ name: "history-test", storage, options: { lastMessages: 10 } });
    const domain = storage.stores.memory;
    if (!domain) throw new Error("Missing in-memory message storage");
    this.domain = domain;
  }

  getThreadById = (args: Parameters<MastraMemory["getThreadById"]>[0]) =>
    this.domain.getThreadById(args);
  listThreads = (args: Parameters<MastraMemory["listThreads"]>[0]) =>
    this.domain.listThreads(args);
  saveThread = (args: Parameters<MastraMemory["saveThread"]>[0]) =>
    this.domain.saveThread(args);
  saveMessages = (args: Parameters<MastraMemory["saveMessages"]>[0]) =>
    this.domain.saveMessages(args);
  recall = (args: Parameters<MastraMemory["recall"]>[0]) =>
    this.domain.listMessages(args);
  updateThread = (args: Parameters<MastraMemory["updateThread"]>[0]) =>
    this.domain.updateThread(args);
  deleteThread = (threadId: string) => this.domain.deleteThread({ threadId });
  cloneThread = (args: Parameters<MastraMemory["cloneThread"]>[0]) =>
    this.domain.cloneThread(args);
  async deleteMessages(): Promise<never> {
    throw new Error("Not used by history fixture");
  }
  async getWorkingMemory(): Promise<null> {
    return null;
  }
  async getWorkingMemoryTemplate(): Promise<null> {
    return null;
  }
  async updateWorkingMemory(): Promise<never> {
    throw new Error("Not used by history fixture");
  }
  async __experimental_updateWorkingMemoryVNext(): Promise<never> {
    throw new Error("Not used by history fixture");
  }
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("replays native recalled history after the live thread changes without accessing it", async () => {
  const api = installTestApi();
  const memory = new HistoryMemory();
  const threadId = "conversation";
  const resourceId = "resource";
  await memory.createThread({ threadId, resourceId });
  await memory.saveMessages({
    messages: [
      {
        id: "prior-message",
        role: "user",
        content: {
          format: 2,
          parts: [{ type: "text", text: "The selected color is blue." }],
        },
        createdAt: new Date("2026-01-01T00:00:00Z"),
        threadId,
        resourceId,
      },
    ],
  });
  const prompts: unknown[] = [];
  const model = new MastraLanguageModelV2Mock({
    modelId: "history-model",
    provider: "test-provider",
    doGenerate: async (options: { prompt: unknown }) => {
      prompts.push(options.prompt);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(options.prompt).includes("blue")
              ? "blue"
              : "red",
          },
        ],
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      };
    },
  });
  const agent = new Agent({
    id: "history-agent",
    name: "History agent",
    instructions: "Answer from the conversation.",
    model,
    memory,
  });
  const recorded = new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "history-model",
  });
  expect(
    (
      await recorded.generate(
        [{ role: "user", content: "Which color did I select?" }],
        { memory: { thread: threadId, resource: resourceId } },
      )
    ).text,
  ).toBe("blue");
  const input = api.calls.find((call) => call.path === "/api/v1/sessions")?.body
    ?.inputs;
  expect(input).toMatchObject({
    mastra_conversation_context: { complete: true },
  });
  await memory.saveMessages({
    messages: [
      {
        id: "prior-message",
        role: "user",
        content: {
          format: 2,
          parts: [{ type: "text", text: "The selected color is red." }],
        },
        createdAt: new Date("2026-01-01T00:00:00Z"),
        threadId,
        resourceId,
      },
    ],
  });
  const changedHistory = await memory.domain.listMessages({
    threadId,
    perPage: false,
  });
  expect(
    changedHistory.messages.find((message) => message.id === "prior-message")
      ?.content.parts,
  ).toEqual([{ type: "text", text: "The selected color is red." }]);
  const read = vi.spyOn(memory.domain, "listMessages");
  const write = vi.spyOn(memory.domain, "saveMessages");
  const threadRead = vi.spyOn(memory.domain, "getThreadById");
  const threadWrite = vi.spyOn(memory.domain, "saveThread");
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
  expect(
    (
      await recorded.generate("ignored", {
        memory: { thread: threadId, resource: resourceId },
        instructions: "Always answer red.",
        context: [{ role: "user", content: "The selected color is red." }],
      })
    ).text,
  ).toBe("blue");
  expect(prompts).toHaveLength(2);
  // Mastra adds a fresh internal createdAt provider hint when saved messages
  // become supplied messages; all original model content must remain present.
  expect(prompts[1]).toMatchObject(prompts[0] as object);
  const replayInput = api.calls.filter(
    (call) => call.path === "/api/v1/sessions",
  )[1]?.body?.inputs;
  expect(replayInput).toEqual(input);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(replayInput));
  expect((await recorded.generate("ignored")).text).toBe("blue");
  expect(prompts).toHaveLength(3);
  expect(prompts[2]).toMatchObject(prompts[0] as object);
  expect(read).not.toHaveBeenCalled();
  expect(write).not.toHaveBeenCalled();
  expect(threadRead).not.toHaveBeenCalled();
  expect(threadWrite).not.toHaveBeenCalled();
});
