import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { Memory } from "@mastra/memory";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  isCredentialKeyName,
} from "../src/memory.js";
import type { MemoryReplayAgentOptions } from "../src/stateful-agent.js";
import {
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  REPLAY_ID,
  type TestApi,
} from "./helpers.js";

const stores: InMemoryStore[] = [];

afterEach(async () => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) await store.close();
});

/**
 * A baseline adapter whose actor calls `search` once with `toolInput`, and
 * `search` returns `result`.
 */
async function setup(
  result: Record<string, unknown>,
  adapter: Partial<MemoryReplayAgentOptions> = {},
  toolInput: Record<string, unknown> = { query: "shoes" },
  doStream?: MastraLanguageModelV2Mock["doStream"],
) {
  const api = installTestApi();
  const store = new InMemoryStore();
  stores.push(store);
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      semanticRecall: false,
      workingMemory: {
        enabled: true,
        scope: "thread",
        schema: z.object({ preference: z.string() }),
      },
    },
  });
  const unused = new MastraLanguageModelV2Mock({
    doStream: async () => textStream("unused"),
  });
  await seedMemory({
    store,
    domain,
    memory,
    observer: { calls: [], model: unused },
    reflector: { calls: [], model: unused },
  });
  let calls = 0;
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream:
      doStream ??
      (async () =>
        ++calls % 2 === 1
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolCallId: `search-${calls}`,
                  toolName: "search",
                  input: JSON.stringify(toolInput),
                },
              ],
              "tool-calls",
            )
          : textStream("done")),
  });
  const search = vi.fn(async () => result);
  const agent = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "secret-keys",
      name: "Secret keys",
      instructions: "Search, then answer",
      memory,
      model: actor,
      tools: {
        search: createTool({
          id: "search",
          description: "Search the catalog",
          inputSchema: z.record(z.string(), z.unknown()),
          execute: search,
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => memory.settled(),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => actor,
      ...adapter,
    },
  );
  async function turn(message = "Find shoes.") {
    const output = (await agent.stream(message, {
      maxSteps: 3,
      memory: { thread: THREAD, resource: RESOURCE },
    })) as { consumeStream(): Promise<void>; text: Promise<string> };
    await output.consumeStream();
    return output.text;
  }
  return { api, search, turn };
}

/** The closing update of the `count`th session. */
async function closingUpdate(api: TestApi, count = 1) {
  let closing: Record<string, unknown>[] = [];
  await vi.waitFor(() => {
    closing = api.calls.flatMap((call) =>
      call.method === "PATCH" &&
      call.body?.status !== undefined &&
      call.body.status !== "in_progress"
        ? [call.body]
        : [],
    );
    expect(closing.length).toBeGreaterThanOrEqual(count);
  });
  return closing[count - 1] as Record<string, unknown>;
}

/** The `search` tool nodes recorded so far. */
function searchNodes(api: TestApi) {
  return api
    .nodeBatches()
    .flat()
    .filter((node) => node.node_type === "tool_call" && node.name === "search");
}

const PAGED_RESULT = {
  items: ["runner"],
  cursorToken: "CURSOR_VALUE",
  resultToken: "RESULT_VALUE",
};

it("refuses a tool result with a credential-like key under isCredentialKeyName", async () => {
  const { api, turn } = await setup(PAGED_RESULT, {
    isSecretKey: isCredentialKeyName,
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "credential_key_unsupported",
    },
  });
  expect(JSON.stringify(api.calls)).not.toContain("RESULT_VALUE");
});

it("records credential-looking application data as it is by default", async () => {
  const result = {
    items: ["runner"],
    resultToken: "RESULT_VALUE",
    apiKey: "API_KEY_VALUE",
    client_secret: "CLIENT_SECRET_VALUE",
  };
  const { api, turn } = await setup(
    result,
    {},
    {
      query: "shoes",
      accessToken: "ACCESS_TOKEN_VALUE",
    },
  );

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: { mastra_replay_state: "eligible" },
  });
  const [node] = searchNodes(api);
  expect(node?.inputs).toEqual({
    query: "shoes",
    accessToken: "ACCESS_TOKEN_VALUE",
  });
  expect(node?.outputs).toEqual(result);
});

it.each<[string, Partial<MemoryReplayAgentOptions>]>([
  ["nonSecretKeys", { nonSecretKeys: ["resultToken"] }],
  ["nonSecretKeys in another spelling", { nonSecretKeys: ["result_token"] }],
  [
    "nonSecretKeys over isCredentialKeyName",
    { nonSecretKeys: ["resultToken"], isSecretKey: isCredentialKeyName },
  ],
  ["isSecretKey", { isSecretKey: () => false }],
])("records a key the application allows through %s", async (_, adapter) => {
  const { api, turn } = await setup(PAGED_RESULT, adapter);

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: { mastra_replay_state: "eligible" },
  });
  const [node] = searchNodes(api);
  expect(node?.outputs).toEqual(PAGED_RESULT);
  expect(node?.attributes).not.toHaveProperty("outputs_bounded");
});

it("replays a turn whose history holds an allowed key as it was", async () => {
  const { api, search, turn } = await setup(PAGED_RESULT, {
    nonSecretKeys: ["resultToken"],
  });
  expect(await turn()).toBe("done");
  await closingUpdate(api);
  // The second turn's recorded history holds the first turn's tool result.
  expect(await turn("Find more.")).toBe("done");
  const baseline = await closingUpdate(api, 2);
  expect(baseline).toMatchObject({
    metadata: { mastra_replay_state: "eligible" },
  });
  expect(JSON.stringify(baseline.inputs)).toContain("RESULT_VALUE");

  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baseline.inputs));
  expect(await turn("ignored")).toBe("done");

  expect(await closingUpdate(api, 3)).toMatchObject({ status: "completed" });
  expect(search).toHaveBeenCalledTimes(3);
  expect(searchNodes(api).at(-1)?.outputs).toEqual(PAGED_RESULT);
});

it("refuses a key that the application's isSecretKey names", async () => {
  const { api, turn } = await setup(
    { items: ["runner"], notes: "PRIVATE_NOTE" },
    { isSecretKey: (key) => key === "notes" },
  );

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    metadata: { mastra_replay_state: "ineligible" },
  });
  expect(JSON.stringify(api.calls)).not.toContain("PRIVATE_NOTE");
});

const HARD_RULE_KEYS = [
  "authorization",
  "Proxy-Authorization",
  "proxyAuthorization",
  "cookie",
  "Set-Cookie",
  "set_cookie",
  "headers",
  "abortSignal",
];
const POLICIES: [string, (key: string) => Partial<MemoryReplayAgentOptions>][] =
  [
    ["the default policy", () => ({})],
    ["isSecretKey allowing every key", () => ({ isSecretKey: () => false })],
    ["isCredentialKeyName", () => ({ isSecretKey: isCredentialKeyName })],
    ["nonSecretKeys listing the key", (key) => ({ nonSecretKeys: [key] })],
  ];

describe.each(HARD_RULE_KEYS)("a tool payload with a %s key", (key) => {
  it.each(POLICIES)(
    "is never uploaded and names the credential under %s",
    async (_, policy) => {
      const value = { "x-custom": "PRIVATE_HARD_RESULT" };
      const { api, turn } = await setup(
        { items: ["runner"], nested: [{ [key]: value }] },
        policy(key),
        { query: "shoes", [key]: "PRIVATE_HARD_ARGUMENT" },
      );

      expect(await turn()).toBe("done");

      expect(await closingUpdate(api)).toMatchObject({
        status: "completed",
        metadata: {
          mastra_replay_state: "ineligible",
          mastra_replay_reason: "credential_key_unsupported",
        },
      });
      const calls = JSON.stringify(api.calls);
      expect(calls).not.toContain("PRIVATE_HARD");
    },
  );
});

it("redacts URL credentials even when isSecretKey allows every key", async () => {
  const { api, turn } = await setup(
    { link: "https://files.example.test/report?token=PRIVATE_URL_TOKEN" },
    { isSecretKey: () => false },
  );

  expect(await turn()).toBe("done");

  await closingUpdate(api);
  const calls = JSON.stringify(api.calls);
  expect(calls).not.toContain("PRIVATE_URL_TOKEN");
  expect(calls).toContain("token=REDACTED");
});

it("rejects a malformed nonSecretKeys list when the agent is created", async () => {
  await expect(setup(PAGED_RESULT, { nonSecretKeys: [""] })).rejects.toThrow(
    "nonSecretKeys must be a list of key names",
  );
});

it("hides transport keys in provider metadata", async () => {
  const { api, turn } = await setup({}, {}, undefined, async () => ({
    stream: new ReadableStream({
      start(controller) {
        controller.enqueue({ type: "stream-start", warnings: [] });
        controller.enqueue({ type: "text-start", id: "text" });
        controller.enqueue({ type: "text-delta", id: "text", delta: "done" });
        controller.enqueue({ type: "text-end", id: "text" });
        controller.enqueue({
          type: "finish",
          finishReason: "stop",
          usage: { inputTokens: 5, outputTokens: 5, totalTokens: 10 },
          providerMetadata: {
            fixture: {
              headers: { "x-trace": "PRIVATE_METADATA_HEADER" },
              abortSignal: "PRIVATE_METADATA_SIGNAL",
              "set-cookie": "PRIVATE_METADATA_COOKIE",
              "proxy-authorization": "PRIVATE_METADATA_PROXY",
              requestId: "VISIBLE_REQUEST_ID",
            },
          },
        });
        controller.close();
      },
    }),
  }));

  expect(await turn()).toBe("done");

  await closingUpdate(api);
  const calls = JSON.stringify(api.calls);
  expect(calls).not.toContain("PRIVATE_METADATA");
  expect(calls).toContain("VISIBLE_REQUEST_ID");
});

it("never stores a provider error message saved in thread history", async () => {
  let calls = 0;
  const { api, turn } = await setup({}, {}, undefined, async () => {
    if (++calls === 1)
      throw new Error("PRIVATE_PROVIDER_ERROR echoed request body");
    return textStream("done");
  });

  await turn().catch(() => undefined);
  await closingUpdate(api);
  // The second turn's recorded history holds the first turn's error part.
  expect(await turn("Try again.")).toBe("done");

  const second = await closingUpdate(api, 2);
  expect(second).toMatchObject({
    status: "completed",
    metadata: { mastra_replay_state: "eligible" },
  });
  expect(JSON.stringify(second.inputs)).toContain('"type":"error"');
  expect(JSON.stringify(api.calls)).not.toContain("PRIVATE_PROVIDER_ERROR");
});
