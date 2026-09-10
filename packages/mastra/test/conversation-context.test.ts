import { MessageList } from "@mastra/core/agent/message-list";
import type { InputProcessor } from "@mastra/core/processors";
import { RequestContext } from "@mastra/core/request-context";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createContextInput } from "../src/conversation-context.js";
import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  REPLAY_ID,
  textStep,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function wrap(agent: FakeAgent) {
  return new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "model",
  });
}

describe("recorded conversation context", () => {
  it("answers from the original prior turn after live history changes, without reading or writing that history", async () => {
    const api = installTestApi();
    let history = "The selected color is blue.";
    let reads = 0;
    let writes = 0;
    const agent = new FakeAgent(async (supplied, options) => {
      const messages = new MessageList();
      if (options.memory) {
        reads++;
        messages.add([{ role: "user", content: history }], "memory");
      }
      messages.add(supplied as string, "input");
      for (const processor of (options.inputProcessors ??
        []) as InputProcessor[]) {
        await processor.processInputStep?.({ messageList: messages } as never);
      }
      const text = JSON.stringify(messages.get.all.aiV5.model()).includes(
        "blue",
      )
        ? "blue"
        : "red";
      if (options.memory) writes++;
      await options.onStepFinish?.(textStep());
      return { text };
    });
    const recorded = await wrap(agent).generate("Which color did I select?", {
      memory: { thread: "thread", resource: "resource" },
    });
    const input = api.calls.find((call) => call.path === "/api/v1/sessions")
      ?.body?.inputs;
    expect(recorded).toEqual({ text: "blue" });
    expect(input).toMatchObject({
      supplied_messages: "Which color did I select?",
      mastra_conversation_context: { complete: true, source: "recalled" },
    });
    history = "The selected color is red.";
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    expect(
      await wrap(agent).generate("ignored", {
        memory: { thread: "thread", resource: "resource" },
      }),
    ).toEqual({ text: "blue" });
    const replayInput = api.calls.filter(
      (call) => call.path === "/api/v1/sessions",
    )[1]?.body?.inputs;
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(replayInput));
    expect(
      await wrap(agent).generate("ignored again", { threadId: "thread" }),
    ).toEqual({ text: "blue" });
    expect(reads).toBe(1);
    expect(writes).toBe(1);
    expect(agent.calls[1]?.messages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          role: "user",
          content: expect.arrayContaining([
            expect.objectContaining({ text: "The selected color is blue." }),
          ]),
        }),
      ]),
    );
  });

  it("preserves supplied message arrays without claiming recalled history", async () => {
    const api = installTestApi();
    const messages = [
      { role: "user", content: "Choose blue" },
      { role: "assistant", content: "OK" },
      { role: "user", content: "Which color?" },
    ];
    const agent = new FakeAgent();
    await wrap(agent).generate(messages);
    expect(api.calls[0]?.body?.inputs).toEqual(messages);
    expect(agent.calls[0]?.messages).toBe(messages);
  });

  it("refuses legacy memory-dependent input before the agent executes", async () => {
    installTestApi();
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const agent = new FakeAgent();
    await expect(
      wrap(agent).generate("Which color?", { threadId: "thread" }),
    ).rejects.toThrow("complete conversation context is unavailable");
    expect(agent.calls).toHaveLength(0);
  });

  it("refuses incomplete or lossy context before execution", async () => {
    installTestApi();
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv(
      "KITARU_TASK_INPUTS",
      JSON.stringify(
        createContextInput("input", [
          { role: "user", content: "hello", password: "credential" },
        ]),
      ),
    );
    const agent = new FakeAgent();
    await expect(wrap(agent).generate("ignored")).rejects.toThrow(
      "complete conversation context is unavailable",
    );
    expect(agent.calls).toHaveLength(0);
  });

  it.each([
    "prompt",
    "system_prompt",
  ])("rejects %s overrides rather than changing the saved context", async (field) => {
    const api = installTestApi({
      replaySpec: {
        baseline_session_id: "018f0000-0000-7000-8000-000000000102",
        id: REPLAY_ID,
        override: { [field]: "replacement" },
        status: "pending",
        tool_policy: { default: { type: "passthrough" }, tools: {} },
      },
    });
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv(
      "KITARU_TASK_INPUTS",
      JSON.stringify(
        createContextInput("input", [{ role: "user", content: "original" }]),
      ),
    );
    const agent = new FakeAgent();
    await expect(wrap(agent).generate("ignored")).rejects.toThrow(
      "overrides cannot replace a recorded conversation context",
    );
    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it.each([
    { workingMemory: { enabled: true } },
    { semanticRecall: true },
    { observationalMemory: true },
  ])("records advanced memory as unsupported without breaking the original invocation: %j", async (memoryConfig) => {
    const api = installTestApi();
    const requestContext = new RequestContext();
    requestContext.set("MastraMemory", { memoryConfig });
    const agent = new FakeAgent(async (_input, options) => {
      const messageList = new MessageList();
      messageList.add("hello", "input");
      for (const processor of (options.inputProcessors ??
        []) as InputProcessor[]) {
        await processor.processInputStep?.({
          messageList,
          requestContext,
        } as never);
      }
      return { text: "original answer" };
    });
    expect(await wrap(agent).generate("hello", { requestContext })).toEqual({
      text: "original answer",
    });
    const input = api.calls[0]?.body?.inputs;
    expect(input).toMatchObject({
      mastra_conversation_context: { complete: false },
    });
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    await expect(wrap(agent).generate("ignored")).rejects.toThrow(
      "complete conversation context is unavailable",
    );
    expect(agent.calls).toHaveLength(1);
  });

  it("refuses restoring a snapshot outside a replay before live history can run", async () => {
    const api = installTestApi();
    const agent = new FakeAgent();
    const input = createContextInput("hello", [
      { role: "user", content: "hello" },
    ]);
    await expect(
      wrap(agent).generate(input, { threadId: "live" }),
    ).rejects.toThrow("can only be restored through a Kitaru replay");
    expect(agent.calls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("marks context unavailable when the framework never reaches the capture hook", async () => {
    const api = installTestApi();
    const agent = new FakeAgent(async () => {
      throw new Error("input rejected");
    });
    await expect(
      wrap(agent).generate("input", { threadId: "thread" }),
    ).rejects.toThrow("input rejected");
    expect(api.calls[0]?.body?.inputs).toMatchObject({
      mastra_conversation_context: { complete: false },
    });
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
  });
});
