import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { MAX_MASTRA_REPLAY_JSON_BYTES } from "@zenml-io/kitaru/adapter";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  MEMORY_REPLAY_KEY,
} from "../src/memory.js";
import {
  finalizeMemoryReplayEnvelope,
  type MastraMemoryReplayEnvelope,
} from "../src/memory-snapshot.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  settleBuffering,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

const stores: ReturnType<typeof createMemoryRuntime>["store"][] = [];

function withoutDecisions(inputs: Record<string, unknown>) {
  const envelope = inputs[MEMORY_REPLAY_KEY] as MastraMemoryReplayEnvelope;
  return {
    ...inputs,
    [MEMORY_REPLAY_KEY]: finalizeMemoryReplayEnvelope(
      envelope,
      envelope.omTape,
      (value) => {
        if (
          value === null ||
          typeof value !== "object" ||
          Array.isArray(value)
        ) {
          throw new Error("Expected a replay envelope object");
        }
        const { processorDecisions: _decisions, ...legacy } = value;
        return legacy;
      },
    ),
  };
}

afterEach(async () => {
  await settleBuffering();
  await Promise.all(stores.splice(0).map((store) => store.close()));
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

async function fixture(
  mode?: unknown,
  toolStep = false,
  modelParams: Record<string, unknown> = {},
) {
  const runtime = createMemoryRuntime({ messageTokens: 100000 });
  stores.push(runtime.store);
  await seedMemory(runtime);
  const replaySpec: Record<string, unknown> = {
    id: REPLAY_ID,
    baseline_session_id: ORIGINAL_SESSION_ID,
    status: "pending",
    override:
      mode === undefined && Object.keys(modelParams).length === 0
        ? null
        : {
            model_params: {
              ...modelParams,
              ...(mode === undefined ? {} : { mastraProcessorDecisions: mode }),
            },
          },
    tool_policy: { default: { type: "passthrough" }, tools: {} },
  };
  const api = installTestApi({ replaySpec });
  let skill = "historical-skill";
  let decisionPayload: string | undefined;
  const reported = vi.fn();
  const classifier = vi.fn(async () => ({
    content: [{ type: "text" as const, text: skill }],
    finishReason: "stop" as const,
    usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 },
    warnings: [],
    response: { id: "classifier-response", modelId: "served-classifier" },
  }));
  const decide = vi.fn();
  const preceding = vi.fn();
  const actor = vi.fn(async (_options: unknown) => {
    if (toolStep && actor.mock.calls.length === 1) {
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: "lookup-1",
            toolName: "lookup",
            input: "{}",
          },
        ],
        "tool-calls",
      );
    }
    return textStream("done");
  });
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: actor,
  });
  const adapter = createMemoryReplayAgent(
    ({ memory, decisions }) => {
      const router = decisions.define("skill-router");
      const classifierModel = router.instrumentModel(
        new MastraLanguageModelV2Mock({
          modelId: "classifier",
          provider: "fixture",
          doGenerate: classifier,
        }),
      );
      return {
        id: "skill-routing",
        name: "Skill routing",
        instructions: "Answer using the selected skill",
        model,
        memory,
        defaultOptions: { maxSteps: 3 },
        tools: toolStep
          ? {
              lookup: createTool({
                id: "lookup",
                description: "Look up a value",
                inputSchema: z.object({}),
                execute: async () => ({ value: "found" }),
              }),
            }
          : undefined,
        inputProcessors: [
          {
            id: "preceding",
            processInput: ({ messages }) => {
              preceding();
              return messages;
            },
          },
          {
            id: "skill-router",
            async processInput({ messages }) {
              const selected = await router.run(async () => {
                decide();
                const result = await classifierModel.doGenerate({
                  prompt: [
                    {
                      role: "user",
                      content: [{ type: "text", text: "Choose a skill" }],
                    },
                  ],
                });
                return {
                  skillIds: result.content.flatMap((part) =>
                    part.type === "text" ? [part.text] : [],
                  ),
                  ...(decisionPayload === undefined
                    ? {}
                    : { diagnostic: decisionPayload }),
                };
              });
              return messages.map((message) => ({
                ...message,
                content: {
                  ...message.content,
                  parts: [
                    ...message.content.parts,
                    {
                      type: "text" as const,
                      text: `Selected skills: ${selected.skillIds.join(", ")}`,
                    },
                  ],
                },
              }));
            },
          },
        ],
      };
    },
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      onRecordingError: reported,
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
      sessionSetupWaitMs: 20,
    },
  );
  async function baseline() {
    const output = await adapter.stream("Please help", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    await vi.waitFor(() =>
      expect(
        api.calls.some(
          (call) =>
            call.method === "PATCH" &&
            (call.body?.metadata as Record<string, unknown> | undefined)
              ?.mastra_replay_state === "eligible",
        ),
      ).toBe(true),
    );
    const inputs = api.calls.find(
      (call) =>
        call.method === "PATCH" &&
        (call.body?.metadata as Record<string, unknown> | undefined)
          ?.mastra_replay_state === "eligible",
    )?.body?.inputs;
    if (!inputs) throw new Error("Missing recorded input");
    return inputs as Record<string, unknown>;
  }
  async function replay(inputs: Record<string, unknown>) {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(inputs));
    const result = await adapter.stream("ignored");
    await result.consumeStream();
    return result;
  }
  return {
    api,
    adapter,
    actor,
    classifier,
    decide,
    preceding,
    baseline,
    replay,
    reported,
    setDecisionPayload: (value: string | undefined) => {
      decisionPayload = value;
    },
    setReplayMode: (value: string) => {
      replaySpec.override = {
        model_params: { ...modelParams, mastraProcessorDecisions: value },
      };
    },
    setSkill: (value: string) => {
      skill = value;
    },
  };
}

it("records the router decision and classifier model as a child with token usage", async () => {
  const f = await fixture();
  const inputs = await f.baseline();
  const nodes = f.api.nodeBatches().flat();
  const decision = nodes.find((node) => node.name === "skill-router");
  expect(decision).toMatchObject({ node_type: "span", status: "completed" });
  expect(JSON.stringify(decision?.outputs)).toContain("historical-skill");
  expect(
    nodes.find((node) => node.model === "served-classifier"),
  ).toMatchObject({
    node_type: "llm_call",
    parent_external_id: decision?.external_id,
    tokens: { input_tokens: 7, output_tokens: 3 },
  });
  expect(JSON.stringify(inputs[MEMORY_REPLAY_KEY])).toContain(
    "historical-skill",
  );
  expect(f.classifier).toHaveBeenCalledTimes(1);
});

it("runs the changed classifier by default and feeds its new decision to the actor", async () => {
  const f = await fixture();
  const input = await f.baseline();
  f.setSkill("changed-skill");
  await f.replay(input);
  expect(f.classifier).toHaveBeenCalledTimes(2);
  expect(f.decide).toHaveBeenCalledTimes(2);
  expect(JSON.stringify(f.actor.mock.calls[1])).toContain("changed-skill");
  expect(JSON.stringify(f.actor.mock.calls[1])).not.toContain(
    "mastraProcessorDecisions",
  );
});

it("pins the recorded decision without calling the classifier again", async () => {
  const f = await fixture("pinned");
  const input = await f.baseline();
  f.setSkill("changed-skill");
  await f.replay(input);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(f.decide).toHaveBeenCalledTimes(1);
  expect(f.actor).toHaveBeenCalledTimes(2);
  expect(JSON.stringify(f.actor.mock.calls[1])).toContain("historical-skill");
  expect(JSON.stringify(f.actor.mock.calls[1])).not.toContain("changed-skill");
  expect(JSON.stringify(f.actor.mock.calls[1])).not.toContain(
    "mastraProcessorDecisions",
  );
});

it("preserves provider model settings while consuming the pinned decision option", async () => {
  const f = await fixture("pinned", false, { maxOutputTokens: 100 });
  const input = await f.baseline();
  await f.replay(input);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(f.actor.mock.calls[1]?.[0]).toMatchObject({ maxOutputTokens: 100 });
  expect(JSON.stringify(f.actor.mock.calls[1])).not.toContain(
    "mastraProcessorDecisions",
  );
});

it("keeps memory replay eligible when optional decision output exceeds the combined envelope budget", async () => {
  const f = await fixture();
  f.setDecisionPayload("x".repeat(MAX_MASTRA_REPLAY_JSON_BYTES - 1024));
  const input = await f.baseline();
  expect(input[MEMORY_REPLAY_KEY]).not.toHaveProperty("processorDecisions");
  expect(f.decide).toHaveBeenCalledTimes(1);
  expect(f.reported).toHaveBeenCalled();
  f.setDecisionPayload(undefined);
  f.setSkill("changed-skill");
  await f.replay(input);
  expect(f.classifier).toHaveBeenCalledTimes(2);
  expect(JSON.stringify(f.actor.mock.calls[1])).toContain("changed-skill");
  f.setReplayMode("pinned");
  await expect(f.replay(input)).rejects.toThrow(/decision/i);
  expect(f.classifier).toHaveBeenCalledTimes(2);
  expect(f.actor).toHaveBeenCalledTimes(2);
});

it("refuses missing pinned decisions before preceding processors or any models run", async () => {
  const f = await fixture("pinned");
  const input = await f.baseline();
  await expect(f.replay(withoutDecisions(input))).rejects.toThrow(/decision/i);
  expect(f.preceding).toHaveBeenCalledTimes(1);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(f.actor).toHaveBeenCalledTimes(1);
});

it("replays older baselines without processor decisions in live mode", async () => {
  const f = await fixture();
  const input = await f.baseline();
  f.setSkill("new-router-skill");
  await f.replay(withoutDecisions(input));
  expect(f.classifier).toHaveBeenCalledTimes(2);
  expect(JSON.stringify(f.actor.mock.calls[1])).toContain("new-router-skill");
});

it("rejects unknown decision modes before executing any replay model", async () => {
  const f = await fixture("invalid");
  const input = await f.baseline();
  await expect(f.replay(input)).rejects.toThrow(/mastraProcessorDecisions/);
  expect(f.preceding).toHaveBeenCalledTimes(1);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(f.actor).toHaveBeenCalledTimes(1);
});

it("runs the input decision once when the actor continues after a tool call", async () => {
  const f = await fixture(undefined, true);
  await f.baseline();
  expect(f.actor).toHaveBeenCalledTimes(2);
  expect(f.decide).toHaveBeenCalledTimes(1);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(
    f.api
      .nodeBatches()
      .flat()
      .filter((node) => node.name === "skill-router"),
  ).toHaveLength(1);
});

it("isolates the same decision name in overlapping turns", async () => {
  const f = await fixture();
  let releaseFirst: () => void = () => {};
  const firstCanFinish = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let classifierCalls = 0;
  f.classifier.mockImplementation(async () => {
    const call = ++classifierCalls;
    if (call === 1) await firstCanFinish;
    else releaseFirst();
    return {
      content: [{ type: "text", text: `skill-${call}` }],
      finishReason: "stop",
      usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 },
      warnings: [],
      response: { id: `classifier-${call}`, modelId: "served-classifier" },
    };
  });
  await Promise.all(
    ["first", "second"].map(async (thread) => {
      const result = await f.adapter.stream(`Please help ${thread}`, {
        memory: { thread, resource: thread },
      });
      await result.consumeStream();
    }),
  );
  await vi.waitFor(() =>
    expect(
      f.api.calls.filter(
        (call) =>
          call.method === "PATCH" &&
          (call.body?.metadata as Record<string, unknown> | undefined)
            ?.mastra_replay_state === "eligible",
      ),
    ).toHaveLength(2),
  );
  expect(f.api.sessionIds).toHaveLength(2);
  const decisions = f.api.sessionIds.map((session) => {
    const nodes = f.api.nodeBatches(session).flat();
    const decision = nodes.find((node) => node.name === "skill-router");
    expect(decision).toMatchObject({ node_type: "span", status: "completed" });
    expect(
      nodes.find((node) => node.model === "served-classifier"),
    ).toMatchObject({
      parent_external_id: decision?.external_id,
    });
    return JSON.stringify(decision?.outputs);
  });
  expect(decisions.join(" ")).toContain("skill-1");
  expect(decisions.join(" ")).toContain("skill-2");
  expect(new Set(decisions).size).toBe(2);
});

it("runs the native callback once when Kitaru session recording is unavailable", async () => {
  const f = await fixture();
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => {
      throw new Error("Kitaru unavailable");
    }),
  );
  const output = await f.adapter.stream("Please help", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  expect(await output.text).toBe("done");
  expect(f.decide).toHaveBeenCalledTimes(1);
  expect(f.classifier).toHaveBeenCalledTimes(1);
  expect(f.actor).toHaveBeenCalledTimes(1);
});
