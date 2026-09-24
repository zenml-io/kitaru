import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { InputProcessor } from "@mastra/core/processors";
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
  FILE_URL,
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

afterEach(async () => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  await settleBuffering();
});

it("runs the native file processor with historical bytes, skills and complete large request evidence", async () => {
  const signedFileUrl = `${FILE_URL}?token=HISTORICAL_SECRET`;
  const directory = await mkdtemp(join(tmpdir(), "kitaru-stateful-files-"));
  await mkdir(join(directory, "triage"));
  await writeFile(
    join(directory, "triage", "SKILL.md"),
    "---\nname: triage\ndescription: HISTORICAL_SKILL.\n---\nUse historical knowledge.\n",
  );
  const runtime = createMemoryRuntime({ messageTokens: 100000 });
  await seedMemory(runtime);
  const nativeFetch = globalThis.fetch;
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: { system_prompt: "Changed application instruction" },
      tool_policy: {
        default: { type: "history", on_miss: "fail", scope: "baseline" },
        tools: {},
      },
    },
  });
  const apiFetch = globalThis.fetch;
  vi.stubGlobal("fetch", ((
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ) =>
    String(input).startsWith("data:")
      ? nativeFetch(input, init)
      : apiFetch(input, init)) as typeof fetch);
  const requests: unknown[] = [];
  const bytes = new Uint8Array(40000).fill(65);
  const fetchFile = vi.fn(async () => ({
    bytes,
    mediaType: "application/pdf",
  }));
  const processFile = vi.fn();
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async (args) => {
      requests.push(args);
      return textStream("done");
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory, workspace, resolveFile }) => ({
      id: "files",
      name: "Files",
      instructions: "Original application instruction",
      model,
      memory,
      workspace,
      inputProcessors: [
        {
          id: "file-content",
          async processInput({ messages }) {
            processFile();
            const part = messages
              .flatMap((message) => message.content.parts)
              .find((item) => item.type === "file");
            if (part?.type !== "file") throw new Error("Missing file");
            const content = await resolveFile(String(part.data));
            return messages.map((message) => ({
              ...message,
              content: {
                ...message.content,
                parts: message.content.parts.map((part) =>
                  part.type === "file"
                    ? {
                        ...part,
                        data: Buffer.from(content.bytes).toString("base64"),
                      }
                    : part,
                ),
              },
            }));
          },
        },
      ],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : model,
      files: [signedFileUrl],
      resolveFile: fetchFile,
      skillsDirectory: directory,
    },
  );
  try {
    const baseline = await adapter.stream(
      [
        {
          role: "user",
          content: [
            { type: "text", text: "Please read" },
            {
              type: "file",
              data: new URL(signedFileUrl),
              mimeType: "application/pdf",
            },
          ],
        },
      ],
      {
        memory: { thread: THREAD, resource: RESOURCE },
        system: "Extra context",
      },
    );
    await baseline.consumeStream();
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
    const input = api.calls.find(
      (call) =>
        call.method === "PATCH" &&
        (call.body?.metadata as Record<string, unknown> | undefined)
          ?.mastra_replay_state === "eligible",
    )?.body?.inputs;
    expect(
      (input as Record<string, { complete: boolean }>)[MEMORY_REPLAY_KEY]
        ?.complete,
    ).toBe(true);
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body?.status,
    ).toBe("completed");
    fetchFile.mockRejectedValue(new Error("Original file unavailable"));
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    expect(fetchFile).toHaveBeenCalledTimes(1);
    expect(processFile).toHaveBeenCalledTimes(2);
    const json = JSON.stringify(requests[1]);
    expect(json).toContain("HISTORICAL_SKILL");
    expect(json).toContain("historical-blue");
    expect(json).toContain("Extra context");
    expect(json).toContain("Changed application instruction");
    const modelNodes = api
      .nodeBatches()
      .flat()
      .filter((node) => node.node_type === "llm_call");
    expect(modelNodes).toHaveLength(2);
    expect(
      modelNodes.every(
        (node) =>
          (node.attributes as Record<string, unknown>).request_complete ===
          true,
      ),
    ).toBe(true);
    expect(JSON.stringify(modelNodes[1]?.inputs).length).toBeGreaterThan(40000);
    expect(JSON.stringify(api.calls)).not.toContain("HISTORICAL_SECRET");
  } finally {
    await runtime.store.close();
    await rm(directory, { recursive: true, force: true });
  }
});

it("keeps signed URLs native while recorded nodes use file references or redacted URLs", async () => {
  const signed = "https://files.invalid/report.pdf?token=NATIVE_SECRET";
  const runtime = createMemoryRuntime({ messageTokens: 100000 });
  await seedMemory(runtime);
  const api = installTestApi();
  const toolInput = vi.fn();
  const reported = vi.fn();
  const modelRequests: unknown[] = [];
  let calls = 0;
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async (args) => {
      modelRequests.push(args);
      return ++calls === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolCallId: "signed-file",
                toolName: "readFile",
                input: JSON.stringify({ url: signed }),
              },
            ],
            "tool-calls",
          )
        : textStream(`Opened ${signed}`);
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "signed-evidence",
      name: "Signed evidence",
      instructions: "Read the URL",
      memory,
      model,
      defaultOptions: { maxSteps: 3 },
      tools: {
        readFile: createTool({
          id: "readFile",
          description: "Read a file",
          inputSchema: z.object({ url: z.string() }),
          execute: async ({ url }) => {
            toolInput(url);
            return { opened: url };
          },
        }),
      },
    }),
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
      files: [signed],
      resolveFile: async () => ({
        bytes: new Uint8Array([1, 2, 3]),
        mediaType: "application/pdf",
      }),
    },
  );
  try {
    const output = await adapter.stream(`Please open ${signed}`, {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    expect(await output.text).toContain(signed);
    expect(JSON.stringify(modelRequests)).toContain("NATIVE_SECRET");
    expect(toolInput).toHaveBeenCalledWith(signed);
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
    expect(reported).not.toHaveBeenCalled();
    expect(JSON.stringify(api.calls)).not.toContain("NATIVE_SECRET");
    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "tool_call");
    expect(JSON.stringify(tool?.inputs)).toContain("kitaru-file://sha256/");
    expect(JSON.stringify(tool?.outputs)).toContain("kitaru-file://sha256/");
    const completion = api.calls.find(
      (call) =>
        call.method === "PATCH" &&
        (call.body?.metadata as Record<string, unknown> | undefined)
          ?.mastra_replay_state === "eligible",
    );
    expect(JSON.stringify(completion?.body?.outputs)).toContain(
      "Opened https://files.invalid/report.pdf?token=REDACTED",
    );
  } finally {
    await runtime.store.close();
  }
});

it("keeps baseline and replay outputs when only the model output holds a credential URL", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 100000 });
  await seedMemory(runtime);
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: { system_prompt: "Answer with the example" },
      tool_policy: { default: { type: "passthrough" }, tools: {} },
    },
  });
  const answer =
    "Call it like: curl 'https://api.example.com/v1/data?api_key=UNDECLARED_SECRET'";
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream(answer),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "unknown-signed-evidence",
      name: "Unknown signed evidence",
      instructions: "Answer",
      model,
      memory,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : model,
    },
  );
  const final = (sessionId: string | undefined) =>
    api.calls
      .filter(
        (call) =>
          call.method === "PATCH" &&
          call.path.endsWith(`/${sessionId}`) &&
          call.body?.status !== "in_progress",
      )
      .at(-1)?.body;
  try {
    const output = await adapter.stream("Hello", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    expect(await output.text).toContain("UNDECLARED_SECRET");
    await vi.waitFor(() =>
      expect(final(api.sessionIds[0])?.status).toBe("completed"),
    );
    const baseline = final(api.sessionIds[0]);
    expect(baseline?.metadata).toMatchObject({
      mastra_replay_state: "eligible",
    });
    expect(JSON.stringify(baseline?.outputs)).toContain("api_key=REDACTED");
    const input = baseline?.inputs;
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    expect(await replay.text).toContain("UNDECLARED_SECRET");
    await vi.waitFor(() =>
      expect(final(api.sessionIds[1])?.status).toBe("completed"),
    );
    expect(JSON.stringify(final(api.sessionIds[1])?.outputs)).toContain(
      "api_key=REDACTED",
    );
    expect(JSON.stringify(api.calls)).not.toContain("UNDECLARED_SECRET");
  } finally {
    await runtime.store.close();
  }
});

it.each(["late", "memory-name-spoof", "copied-memory-id"])(
  "applies history failure to a %s processor tool before it can execute",
  async (kind) => {
    const runtime = createMemoryRuntime({ messageTokens: 10000 });
    await seedMemory(runtime);
    const api = installTestApi({
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
    });
    let replaying = false;
    const execute = vi.fn(async () => ({ sideEffect: true }));
    const toolName = kind === "late" ? "lateTool" : "updateWorkingMemory";
    const processor: InputProcessor = {
      id: "late-tool",
      processInputStep({ tools }) {
        return {
          tools: {
            ...tools,
            [toolName]:
              kind === "copied-memory-id"
                ? {
                    ...(tools?.updateWorkingMemory as Record<string, unknown>),
                    execute,
                  }
                : createTool({
                    id: toolName,
                    description: "An external tool",
                    inputSchema: z.object({}),
                    execute,
                  }),
          },
        };
      },
    };
    const model = new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async () =>
        replaying
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolName,
                  toolCallId: "external-call",
                  input: "{}",
                },
              ],
              "tool-calls",
            )
          : textStream("baseline"),
    });
    const adapter = createMemoryReplayAgent(
      ({ memory }) => ({
        id: "late",
        name: "Late",
        instructions: "Use a tool",
        model,
        memory,
        inputProcessors: [processor],
      }),
      {
        agentId: AGENT_ID,
        apiUrl: "https://kitaru.invalid",
        requestedModelId: "fixture/actor",
        sourceMemory: () => ({
          settled: () => runtime.memory.settled(),
          domain: runtime.domain,
          configuration: runtime.memory.getMergedThreadConfig(),
          exclusiveAccess: createProcessLocalMemoryAccess(),
        }),
        resolveModel: async (id) =>
          id.includes("observer")
            ? runtime.observer.model
            : id.includes("reflector")
              ? runtime.reflector.model
              : model,
      },
    );
    const baseline = await adapter.stream("record", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await baseline.consumeStream();
    const input = api.calls.find(
      (call) => call.path === "/api/v1/sessions" && call.method === "POST",
    )?.body?.inputs;
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
    replaying = true;
    const result = await adapter.stream("ignored");
    await result.consumeStream();
    // The baseline finalizes in the background, so its update can land last.
    await vi.waitFor(() =>
      expect(
        api.calls
          .filter((call) => call.method === "PATCH")
          .map((call) => call.body?.status),
      ).toContain("failed"),
    );
    expect(execute).not.toHaveBeenCalled();
    expect(
      api.calls.filter((call) => call.path.endsWith("tool-lookup")),
    ).toHaveLength(1);
    await runtime.store.close();
  },
);

it("keeps policies separate when processor tools share the same executor", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 10000 });
  await seedMemory(runtime);
  const staticPolicy = (result: string) => ({
    type: "static",
    on_miss: "fail",
    cases: [{ match: null, match_mode: "exact", result }],
  });
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: null,
      tool_policy: {
        default: { type: "history", on_miss: "fail", scope: "baseline" },
        tools: {
          firstAlias: staticPolicy("FIRST"),
          secondAlias: staticPolicy("SECOND"),
        },
      },
    },
  });
  let replaying = false;
  let step = 0;
  const execute = vi.fn(async () => "live");
  const model = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () =>
      replaying && step++ === 0
        ? streamParts(
            [
              {
                type: "tool-call",
                toolName: "firstAlias",
                toolCallId: "first",
                input: "{}",
              },
              {
                type: "tool-call",
                toolName: "secondAlias",
                toolCallId: "second",
                input: "{}",
              },
            ],
            "tool-calls",
          )
        : textStream("done"),
  });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "aliases",
      name: "Aliases",
      instructions: "Use tools",
      model,
      memory,
      inputProcessors: [
        {
          id: "aliases",
          processInputStep({ tools }) {
            return {
              tools: {
                ...tools,
                firstAlias: createTool({
                  id: "firstAlias",
                  description: "First",
                  inputSchema: z.object({}),
                  execute,
                }),
                secondAlias: createTool({
                  id: "secondAlias",
                  description: "Second",
                  inputSchema: z.object({}),
                  execute,
                }),
              },
            };
          },
        },
      ],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : model,
    },
  );
  const baseline = await adapter.stream("record", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await baseline.consumeStream();
  const input = api.calls.find(
    (call) => call.path === "/api/v1/sessions" && call.method === "POST",
  )?.body?.inputs;
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
  replaying = true;
  const replay = await adapter.stream("ignored");
  await replay.consumeStream();
  const tools = api
    .nodeBatches(api.sessionIds[1])
    .flat()
    .filter((node) => node.node_type === "tool_call");
  expect(tools.map((node) => [node.name, node.outputs])).toEqual([
    ["firstAlias", "FIRST"],
    ["secondAlias", "SECOND"],
  ]);
  expect(execute).not.toHaveBeenCalled();
  await runtime.store.close();
});
