import type { AgentConfig } from "@mastra/core/agent";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { Extractor, Memory } from "@mastra/memory";
import { APICallError } from "ai";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
  MEMORY_REPLAY_KEY,
} from "../src/memory.js";
import type { MemoryReplayAgentOptions } from "../src/stateful-agent.js";
import type { StreamRecordingErrorEvent } from "../src/types.js";
import {
  memoryModel,
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, type TestApi } from "./helpers.js";

const stores: InMemoryStore[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const store of stores.splice(0)) await store.close();
});

type MemoryOptions = NonNullable<
  ConstructorParameters<typeof Memory>[0]
>["options"];

const MEMORY_OPTIONS = {
  lastMessages: 20,
  semanticRecall: false,
  workingMemory: {
    enabled: true,
    scope: "thread",
    schema: z.object({ preference: z.string() }),
  },
} satisfies MemoryOptions;

/** A baseline adapter over a seeded InMemoryStore thread. */
async function setup(
  options: {
    memoryOptions?: MemoryOptions;
    actor?: MastraLanguageModelV2Mock;
    tools?: AgentConfig["tools"];
    adapter?: Partial<MemoryReplayAgentOptions>;
    /** Wrap the test API's fetch before the adapter's client reads it. */
    fetch?: (recorded: typeof globalThis.fetch) => typeof globalThis.fetch;
  } = {},
) {
  const api = installTestApi();
  if (options.fetch) vi.stubGlobal("fetch", options.fetch(globalThis.fetch));
  const store = new InMemoryStore();
  stores.push(store);
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  const memory = new Memory({
    storage: store,
    options: options.memoryOptions ?? MEMORY_OPTIONS,
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
  const actor =
    options.actor ??
    new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async () => textStream("done"),
    });
  const reported: StreamRecordingErrorEvent[] = [];
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "diagnostics",
      name: "Diagnostics",
      instructions: "Answer",
      memory,
      model: actor,
      ...(options.tools ? { tools: options.tools } : {}),
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      onRecordingError: (event) => {
        reported.push(event);
      },
      sourceMemory: () => ({
        settled: () => memory.settled(),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => actor,
      ...options.adapter,
    },
  );
  async function turn(message = "Hello.") {
    const output = (await adapter.stream(message, {
      maxSteps: 3,
      memory: { thread: THREAD, resource: RESOURCE },
    })) as { consumeStream(): Promise<void>; text: Promise<string> };
    await output.consumeStream();
    return output.text;
  }
  return { api, domain, memory, reported, turn };
}

/** The closing update of the only session. */
async function closingUpdate(api: TestApi) {
  let body: Record<string, unknown> | undefined;
  await vi.waitFor(() => {
    body = api.calls.findLast(
      (call) =>
        call.method === "PATCH" &&
        call.body?.status !== undefined &&
        call.body.status !== "in_progress",
    )?.body;
    expect(body).toBeDefined();
  });
  return body as Record<string, unknown>;
}

function recordedEnvelope(api: TestApi) {
  const inputs = api.calls.find(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions",
  )?.body?.inputs as Record<string, { complete: boolean; reasons: string[] }>;
  return inputs[MEMORY_REPLAY_KEY];
}

it("names an oversized thread instead of reporting a malformed snapshot", async () => {
  const { api, domain, reported, turn } = await setup();
  const thread = await domain.getThreadById({ threadId: THREAD });
  if (!thread) throw new Error("Missing seeded thread");
  await domain.saveThread({
    thread: {
      ...thread,
      metadata: { rows: Array.from({ length: 210_000 }, (_, index) => index) },
    },
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    outputs: { text: "done" },
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "replay_input_too_large",
      mastra_native_state: "completed",
    },
  });
  const envelope = recordedEnvelope(api);
  expect(envelope?.complete).toBe(false);
  expect(envelope?.reasons.join(" ")).toMatch(
    /Initial memory snapshot exceeds maximum item count 200000/,
  );
  expect(envelope?.reasons.join(" ")).not.toMatch(/Malformed|Unjoined/);
  await vi.waitFor(() =>
    expect(reported[0]).toMatchObject({
      reason: "replay_input_too_large",
      stage: "complete",
    }),
  );
});

it("names a credential-named key in thread history without storing its value", async () => {
  const { api, memory, turn } = await setup();
  await memory.saveMessages({
    messages: [
      {
        id: "credential-message",
        role: "assistant",
        content: {
          format: 2,
          parts: [{ type: "text", text: "Fetched the page." }],
          metadata: { token: "PRIVATE_TOKEN_VALUE" },
        },
        createdAt: new Date("2026-01-01T00:00:01Z"),
        threadId: THREAD,
        resourceId: RESOURCE,
      },
    ],
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "credential_key_unsupported",
    },
  });
  expect(recordedEnvelope(api)?.reasons.join(" ")).toMatch(
    /sensitive key 'token'/,
  );
  expect(JSON.stringify(api.calls)).not.toContain("PRIVATE_TOKEN_VALUE");
});

it("names a custom observational-memory extractor as unsupported configuration", async () => {
  const observer = memoryModel("observer");
  const reflector = memoryModel("reflector");
  const { api, turn } = await setup({
    memoryOptions: {
      ...MEMORY_OPTIONS,
      observationalMemory: {
        scope: "thread",
        observation: {
          model: observer.model,
          messageTokens: 100_000,
          extract: [
            new Extractor({ name: "custom", instructions: "Extract a value." }),
          ],
        },
        reflection: { model: reflector.model, observationTokens: 100_000 },
      },
    },
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "om_config_unsupported",
    },
  });
});

it("reports a setup failure's reason and closes its session with the native outcome", async () => {
  const { api, reported, turn } = await setup({
    adapter: { captureRequestContext: () => ({ apiToken: "PRIVATE" }) },
  });

  expect(await turn()).toBe("done");

  await vi.waitFor(() => expect(reported).toHaveLength(1));
  expect(reported[0]).toMatchObject({
    reason: "credential_key_unsupported",
    sessionId: api.sessionIds[0],
    stage: "setup",
  });
  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "credential_key_unsupported",
      mastra_native_state: "completed",
    },
  });
  expect(JSON.stringify(api.calls)).not.toContain("PRIVATE");
});

it("refuses a compound credential key nested in the captured request context", async () => {
  const { api, turn } = await setup({
    adapter: {
      captureRequestContext: () => ({
        profile: { access_token: "PRIVATE_NESTED_VALUE" },
      }),
    },
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "credential_key_unsupported",
    },
  });
  expect(JSON.stringify(api.calls)).not.toContain("PRIVATE_NESTED_VALUE");
});

it.each(["pageToken", "max_tokens", "keyboardLayout"])(
  "records a request context key that only resembles a credential: %s",
  async (key) => {
    const { api, turn } = await setup({
      adapter: { captureRequestContext: () => ({ [key]: "value" }) },
    });

    expect(await turn()).toBe("done");

    expect(await closingUpdate(api)).toMatchObject({
      status: "completed",
      metadata: { mastra_replay_state: "eligible" },
    });
  },
);

it.each(["accessToken", "api_key"])(
  "refuses a credential request context key: %s",
  async (key) => {
    const { api, turn } = await setup({
      adapter: { captureRequestContext: () => ({ [key]: "PRIVATE" }) },
    });

    expect(await turn()).toBe("done");

    expect(await closingUpdate(api)).toMatchObject({
      status: "completed",
      metadata: {
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "credential_key_unsupported",
      },
    });
    expect(JSON.stringify(api.calls)).not.toContain("PRIVATE");
  },
);

it("closes a setup failure's session as failed when the native turn fails", async () => {
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      throw new APICallError({
        message: "Service Unavailable",
        url: "https://provider.invalid/v1/responses",
        requestBodyValues: {},
        statusCode: 503,
        isRetryable: false,
      });
    },
  });
  const { api, turn } = await setup({
    actor,
    adapter: { captureRequestContext: () => ({ apiToken: "PRIVATE" }) },
  });

  await turn().catch(() => undefined);

  expect(await closingUpdate(api)).toMatchObject({
    status: "failed",
    metadata: {
      mastra_replay_reason: "credential_key_unsupported",
      mastra_native_state: "failed",
    },
  });
});

it("reports unsupported evidence values by their own reason", async () => {
  const { api, turn } = await setup({
    actor: new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: vi
        .fn()
        .mockResolvedValueOnce(
          streamParts(
            [
              {
                type: "tool-call",
                toolCallId: "call-1",
                toolName: "lookup",
                input: "{}",
              },
            ],
            "tool-calls",
          ),
        )
        .mockResolvedValue(textStream("done")),
    }),
    tools: {
      lookup: createTool({
        id: "lookup",
        description: "Look something up",
        inputSchema: z.object({}),
        execute: async () => ({ compute: () => 1 }),
      }),
    },
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "recorded_evidence_unsupported",
    },
  });
});

// Provider messages echo credentials and request content in many shapes.
// The fake credentials are joined at runtime so secret scanners skip them.
const PROVIDER_SECRETS = [
  `api_key=${["AIza", "Test".repeat(8), "123"].join("")}`,
  ["AK", "IA", "TEST".repeat(3), "1234"].join(""),
  '{"client_secret": "test-json-secret-value"}',
  ["ghp", "test".repeat(9)].join("_"),
  ["xoxb", "test", "test", "test"].join("-"),
  ["sk", "test", "private", "key", "1234"].join("-"),
  [{ alg: "HS256" }, { test: "test" }, "test"]
    .map((part) => Buffer.from(JSON.stringify(part)).toString("base64url"))
    .join("."),
  "echoed private prompt text",
];

it.each([
  { statusCode: 503, error: "AI_APICallError, HTTP 503: provider unavailable" },
  {
    statusCode: 401,
    error: "AI_APICallError, HTTP 401: authentication failed",
  },
  { statusCode: 429, error: "AI_APICallError, HTTP 429: rate limited" },
])(
  "records a failed model call's HTTP $statusCode status but not its message",
  async ({ statusCode, error }) => {
    const actor = new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async () => {
        throw new APICallError({
          message: `Request failed: ${PROVIDER_SECRETS.join(" ")}`,
          url: "https://provider.invalid/v1/responses",
          requestBodyValues: {},
          statusCode,
          isRetryable: false,
        });
      },
    });
    const { api, turn } = await setup({ actor });

    await turn().catch(() => undefined);

    expect(await closingUpdate(api)).toMatchObject({
      status: "failed",
      error: expect.stringContaining(error),
    });
    await vi.waitFor(() => {
      const failed = api
        .nodeBatches()
        .flat()
        .filter((node) => node.node_type === "llm_call");
      const ids = new Set(failed.map((node) => node.external_id));
      expect(ids.size).toBe(1);
      const last = failed.at(-1);
      expect(last).toMatchObject({
        status: "failed",
        error: expect.stringContaining(error),
      });
      expect(last?.inputs).not.toBeNull();
    });
    // Mastra saves the provider error into the thread as an assistant
    // message, and the memory evidence records that write as it happened.
    const stored = JSON.stringify(api.calls, (_key, value) =>
      (value as { name?: unknown } | null)?.name === "memory_mutation"
        ? undefined
        : value,
    );
    expect(stored).toContain(error);
    for (const secret of PROVIDER_SECRETS)
      expect(stored).not.toContain(secret.replaceAll('"', '\\"'));
    expect(stored).not.toContain("Request failed");
  },
);

it("stores why the server refused the replay inputs", async () => {
  const { api, reported, turn } = await setup({
    // A server that predates memory replay refuses the inputs field.
    fetch: (recorded) => async (input, init) => {
      const body = init?.body ? JSON.parse(String(init.body)) : undefined;
      if (init?.method === "PATCH" && body && "inputs" in body)
        return new Response(JSON.stringify({ detail: "Extra inputs" }), {
          status: 422,
          headers: { "Content-Type": "application/json" },
        });
      return recorded(input, init);
    },
  });

  expect(await turn()).toBe("done");

  expect(await closingUpdate(api)).toMatchObject({
    status: "completed",
    outputs: { text: "done" },
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "server_rejected_finalization",
      mastra_native_state: "completed",
    },
  });
  await vi.waitFor(() =>
    expect(reported[0]).toMatchObject({
      reason: "server_rejected_finalization",
      stage: "complete",
    }),
  );
});
