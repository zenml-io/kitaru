import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  MEMORY_REPLAY_KEY,
} from "../src/memory.js";
import {
  createMemoryRuntime,
  type MemoryRuntime,
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
  type TestApi,
  type TestApiOptions,
} from "./helpers.js";

const runtimes: MemoryRuntime[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const runtime of runtimes.splice(0)) await runtime.store.close();
});

/** A search result: 1,400 rows of 10 fields, about 14,000 JSON values. */
function recordRows() {
  return Array.from({ length: 1_400 }, (_, row) =>
    Object.fromEntries(
      Array.from({ length: 10 }, (_, field) => [
        `field${field}`,
        `record-${row}-${field}`,
      ]),
    ),
  );
}

/** Grow the seeded thread to 830 messages carrying 15,000 nested values. */
async function seedLongThread(runtime: MemoryRuntime): Promise<void> {
  await seedMemory(runtime);
  await runtime.domain.saveMessages({
    messages: Array.from({ length: 829 }, (_, index) => ({
      id: `long-${index}`,
      role: index % 2 === 0 ? ("user" as const) : ("assistant" as const),
      content: {
        format: 2 as const,
        parts: [{ type: "text" as const, text: `history ${index}` }],
        ...(index === 0
          ? {
              metadata: {
                items: Array.from({ length: 1_500 }, (_, item) =>
                  Object.fromEntries(
                    Array.from({ length: 10 }, (_, field) => [
                      `field${field}`,
                      item * field,
                    ]),
                  ),
                ),
              },
            }
          : {}),
      },
      createdAt: new Date(Date.UTC(2026, 0, 2, 0, 0, index)),
      threadId: THREAD,
      resourceId: RESOURCE,
    })),
  });
}

async function setup(
  options: {
    recordingLimits?: { maxStringChars: number };
    api?: TestApiOptions;
  } = {},
) {
  const api = installTestApi(options.api);
  const runtime = createMemoryRuntime({ messageTokens: 10_000_000 });
  runtimes.push(runtime);
  await seedLongThread(runtime);
  let actorCalls = 0;
  let executions = 0;
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      actorCalls += 1;
      if (actorCalls % 2 === 0) return textStream("done");
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: `search-${actorCalls}`,
            toolName: "searchRecords",
            input: "{}",
          },
        ],
        "tool-calls",
      );
    },
  });
  const exclusiveAccess = createProcessLocalMemoryAccess();
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "record-search",
      name: "Record search",
      instructions: "Answer",
      memory,
      model: actor,
      tools: {
        searchRecords: createTool({
          id: "searchRecords",
          description: "Search records",
          inputSchema: z.object({}),
          execute: async () => {
            executions += 1;
            return recordRows();
          },
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      ...(options.recordingLimits
        ? { recordingLimits: options.recordingLimits }
        : {}),
      onRecordingError: () => undefined,
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess,
      }),
      resolveModel: (id) =>
        id.endsWith("observer")
          ? runtime.observer.model
          : id.endsWith("reflector")
            ? runtime.reflector.model
            : actor,
    },
  );
  async function turn(message: string): Promise<void> {
    const output = await adapter.stream(message, {
      maxSteps: 5,
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    expect(await output.text).toBe("done");
  }
  return { adapter, api, executions: () => executions, turn };
}

/** The final status and replay state of every recorded session. */
function outcomes(api: TestApi): string[] {
  return api.sessionIds.map((id) => {
    const update = api.calls
      .filter(
        (call) =>
          call.method === "PATCH" &&
          call.path.endsWith(id) &&
          call.body?.status !== "in_progress",
      )
      .at(-1);
    const metadata = update?.body?.metadata as
      | Record<string, unknown>
      | undefined;
    return `${String(update?.body?.status)}/${String(metadata?.mastra_replay_state)}`;
  });
}

function nodes(api: TestApi, sessionId: string, name: string) {
  return api
    .nodeBatches(sessionId)
    .flat()
    .filter((node) => node.name === name || node.node_type === name);
}

it("keeps 1,400-row tool turns on an 830-message thread eligible and stores saved messages once", async () => {
  const { api, turn } = await setup();
  for (const message of ["Find records.", "Find more.", "And again."])
    await turn(message);
  await vi.waitFor(
    () =>
      expect(outcomes(api)).toEqual([
        "completed/eligible",
        "completed/eligible",
        "completed/eligible",
      ]),
    { timeout: 5_000 },
  );
  const created = api.calls.find(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  )?.body?.inputs as Record<string, { complete: boolean }>;
  expect(created[MEMORY_REPLAY_KEY]?.complete).toBe(true);
  for (const sessionId of api.sessionIds) {
    const [save] = nodes(api, sessionId, "memory_mutation").filter(
      (node) =>
        (node.attributes as Record<string, unknown>).memory_method ===
        "saveMessages",
    );
    if (!save) throw new Error("Expected a saveMessages node");
    expect(save.attributes).toMatchObject({
      evidence_complete: true,
      evidence_truncated: false,
    });
    // The tool result is stored with the saved arguments, not again as the result.
    expect(JSON.stringify(save.inputs)).toContain("record-1399-9");
    expect(JSON.stringify(save.outputs)).not.toContain("record-1399-9");
    const saved = (save.outputs as { messages: unknown[] }).messages;
    expect(saved.length).toBeGreaterThan(0);
    for (const message of saved)
      expect(message).toEqual({
        savedMessageRef: {
          id: expect.any(String),
          sha256: expect.stringMatching(/^[a-f0-9]{64}$/),
        },
      });
    for (const request of nodes(api, sessionId, "llm_call"))
      expect(request.attributes).toMatchObject({
        request_complete: true,
        request_evidence_truncated: false,
      });
  }
}, 30_000);

it("applies application recordingLimits to request evidence without making the turn ineligible", async () => {
  const { api, turn } = await setup({
    recordingLimits: { maxStringChars: 20 },
  });
  await turn("Find records with a long enough question.");
  await vi.waitFor(
    () => expect(outcomes(api)).toEqual(["completed/eligible"]),
    { timeout: 5_000 },
  );
  const [sessionId] = api.sessionIds;
  const requests = nodes(api, String(sessionId), "llm_call");
  expect(requests.length).toBeGreaterThan(0);
  for (const request of requests) {
    expect(request.attributes).toMatchObject({
      request_complete: false,
      request_evidence_truncated: true,
      request_incomplete_reasons: expect.arrayContaining([
        "Effective model request exceeds the configured recordingLimits and was truncated",
      ]),
    });
    expect(JSON.stringify(request.inputs)).toContain("[truncated]");
  }
}, 30_000);

it("records a 1,400-row tool result whole so replay serves it from history", async () => {
  let history: unknown;
  const { adapter, api, executions, turn } = await setup({
    api: {
      replaySpec: {
        id: REPLAY_ID,
        baseline_session_id: ORIGINAL_SESSION_ID,
        status: "pending",
        override: null,
        tool_policy: {
          default: { type: "history", on_miss: "fail", scope: "baseline" },
          tools: {},
        },
      },
      lookup: () => ({ match: { result: history, status: "completed" } }),
    },
  });
  await turn("Find records.");
  await vi.waitFor(
    () => expect(outcomes(api)).toEqual(["completed/eligible"]),
    { timeout: 5_000 },
  );
  const [baselineId] = api.sessionIds;
  const [recorded] = nodes(api, String(baselineId), "tool_call");
  // The server serves a history lookup only from a result recorded whole.
  expect(recorded?.attributes).not.toHaveProperty("outputs_bounded");
  expect(recorded?.outputs).toEqual(recordRows());
  history = recorded?.outputs;
  const baseline = api.calls.findLast(
    (call) =>
      call.method === "PATCH" &&
      call.path.endsWith(String(baselineId)) &&
      call.body?.status === "completed",
  )?.body;
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baseline?.inputs));
  try {
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(
      () =>
        expect(outcomes(api)).toEqual([
          "completed/eligible",
          "completed/undefined",
        ]),
      { timeout: 5_000 },
    );
    expect(executions()).toBe(1);
    const [served] = nodes(api, String(api.sessionIds[1]), "tool_call");
    expect(served?.outputs).toEqual(recordRows());
  } finally {
    vi.unstubAllEnvs();
  }
}, 30_000);
