import type { InputProcessor } from "@mastra/core/processors";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { Memory } from "@mastra/memory";
import { afterEach, expect, it, vi } from "vitest";
import { createProcessLocalMemoryAccess } from "../src/memory-binding.js";
import {
  createMemoryReplayAgent,
  type MemoryReplayAgentBindings,
  type MemoryReplayFileCall,
} from "../src/stateful-agent.js";
import { textStream } from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

const DOWNLOAD_TOKEN = "3f2b8c1e-7d4a-4e5f-9a6b-0c1d2e3f4a5b";
const ATTACHMENT_URL = `https://firebasestorage.googleapis.com/v0/b/app-bucket/o/uploads%2Fquote.pdf?alt=media&token=${DOWNLOAD_TOKEN}`;
const ATTACHMENT_BYTES = new Uint8Array([37, 80, 68, 70, 45, 55]);
const THREAD = "signed-thread";
const RESOURCE = "signed-resource";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

/** Send file bytes to the model on each step while history keeps the URL. */
function createAttachmentProcessor(
  resolveFile: MemoryReplayAgentBindings["resolveFile"],
): InputProcessor {
  return {
    id: "attachments",
    async processInputStep({ messages }) {
      return {
        messages: await Promise.all(
          messages.map(async (message) => ({
            ...message,
            content: {
              ...message.content,
              parts: await Promise.all(
                message.content.parts.map(async (part) => {
                  if (
                    part.type !== "file" ||
                    !/^[a-z][a-z0-9+.-]*:\/\//i.test(String(part.data))
                  )
                    return part;
                  const file = await resolveFile(String(part.data));
                  return {
                    ...part,
                    data: Buffer.from(file.bytes).toString("base64"),
                  };
                }),
              ),
            },
          })),
        ),
      };
    },
  };
}

/** A thread whose history holds the attachment's signed URL, not its bytes. */
async function seedAttachmentHistory(padding = "", withFilePart = true) {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  await domain.saveThread({
    thread: {
      id: THREAD,
      resourceId: RESOURCE,
      title: "Quote",
      createdAt: new Date(1_000),
      updatedAt: new Date(1_000),
      metadata: {},
    },
  });
  await domain.saveMessages({
    messages: [
      {
        id: "attachment-message",
        threadId: THREAD,
        resourceId: RESOURCE,
        role: "user",
        createdAt: new Date(2_000),
        content: {
          format: 2,
          parts: [
            {
              type: "text",
              text: `Here is my quote: ${ATTACHMENT_URL}${padding}`,
            },
            ...(withFilePart
              ? [
                  {
                    type: "file" as const,
                    data: ATTACHMENT_URL,
                    mimeType: "application/pdf",
                  },
                ]
              : []),
          ],
        },
      },
    ],
  });
  return { store, domain };
}

it("keeps a thread with a signed attachment URL in history replayable on every turn", async () => {
  const nativeFetch = globalThis.fetch;
  const api = installTestApi();
  const apiFetch = globalThis.fetch;
  // Mastra hands base64 file parts to the model as data: URLs it reads back.
  vi.stubGlobal("fetch", ((
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ) =>
    String(input).startsWith("data:")
      ? nativeFetch(input, init)
      : apiFetch(input, init)) as typeof fetch);
  const { store, domain } = await seedAttachmentHistory();
  const prompts: string[] = [];
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: async ({ prompt }) => {
      prompts.push(JSON.stringify(prompt));
      return textStream("The quote covers two nights.");
    },
  });
  const fetchAttachment = vi.fn(async () => ({
    bytes: ATTACHMENT_BYTES,
    mediaType: "application/pdf",
  }));
  const declareFiles = vi.fn((_call: MemoryReplayFileCall) => [ATTACHMENT_URL]);
  const adapter = createMemoryReplayAgent(
    ({ memory, resolveFile }) => ({
      id: "signed-history",
      name: "Signed history",
      instructions: "Answer about the attachment.",
      memory,
      model,
      inputProcessors: [createAttachmentProcessor(resolveFile)],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: async () => {},
        domain,
        configuration: { lastMessages: 20, semanticRecall: false },
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
      files: declareFiles,
      resolveFile: fetchAttachment,
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
  const encodedBytes = Buffer.from(ATTACHMENT_BYTES).toString("base64");
  try {
    const turns = ["What does it cost?", "Which dates?", "Any extras?"];
    for (const [index, text] of turns.entries()) {
      const output = await adapter.stream(text, {
        memory: { thread: THREAD, resource: RESOURCE },
      });
      await output.consumeStream();
      await vi.waitFor(() =>
        expect(final(api.sessionIds[index])?.status).toBe("completed"),
      );
      expect(final(api.sessionIds[index])?.metadata).toMatchObject({
        mastra_replay_state: "eligible",
      });
      expect(prompts.at(-1)).toContain(encodedBytes);
    }
    expect(declareFiles).toHaveBeenCalledTimes(3);
    expect(declareFiles.mock.calls[2]?.[0]).toMatchObject({
      input: "Any extras?",
      options: { memory: { thread: THREAD, resource: RESOURCE } },
    });
    expect(fetchAttachment).toHaveBeenCalledTimes(3);
    const { messages } = await domain.listMessages({
      threadId: THREAD,
      perPage: false,
    });
    expect(JSON.stringify(messages)).toContain(DOWNLOAD_TOKEN);

    const baselineInput = final(api.sessionIds[2])?.inputs;
    fetchAttachment.mockClear();
    fetchAttachment.mockRejectedValue(new Error("Signed URL was fetched"));
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baselineInput));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(() =>
      expect(final(api.sessionIds[3])?.status).toBe("completed"),
    );
    expect(fetchAttachment).not.toHaveBeenCalled();
    expect(declareFiles).toHaveBeenCalledTimes(3);
    expect(prompts.at(-1)).toContain(encodedBytes);
    expect(prompts.at(-1)).toContain("alt=media&token=REDACTED");
    expect(JSON.stringify(api.calls)).not.toContain(DOWNLOAD_TOKEN);
  } finally {
    await store.close();
  }
});

it("records a turn as ineligible when history holds an undeclared attachment URL", async () => {
  const api = installTestApi();
  const { store, domain } = await seedAttachmentHistory();
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: async () => textStream("The quote covers two nights."),
  });
  // The provider reads the URL itself, so the native turn never downloads it.
  Object.assign(model, { supportedUrls: { "*/*": [/^https:\/\//] } });
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "undeclared-history",
      name: "Undeclared history",
      instructions: "Answer about the attachment.",
      memory,
      model,
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      onRecordingError: () => undefined,
      sourceMemory: () => ({
        settled: async () => {},
        domain,
        configuration: { lastMessages: 20, semanticRecall: false },
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
      files: [],
    },
  );
  try {
    const output = await adapter.stream("What does it cost?", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    expect(await output.text).toBe("The quote covers two nights.");
    // Replay would send the redacted URL to the provider, so the turn is
    // refused up front instead of failing every replay.
    await vi.waitFor(() =>
      expect(
        api.calls.find(
          (call) =>
            call.method === "PATCH" && call.body?.status === "completed",
        )?.body?.metadata,
      ).toMatchObject({
        mastra_replay_state: "ineligible",
        mastra_replay_reason: "file_url_undeclared",
      }),
    );
    expect(JSON.stringify(api.calls)).not.toContain(DOWNLOAD_TOKEN);
  } finally {
    await store.close();
  }
});

/**
 * Resolve the attachment URL `select` reads from a message part, and send a
 * file part's bytes instead of its URL.
 */
function createUrlResolvingProcessor(
  resolveFile: MemoryReplayAgentBindings["resolveFile"],
  select: (part: { type: string; text?: string; data?: unknown }) => unknown,
): InputProcessor {
  return {
    id: "url-attachments",
    async processInputStep({ messages }) {
      return {
        messages: await Promise.all(
          messages.map(async (message) => ({
            ...message,
            content: {
              ...message.content,
              parts: await Promise.all(
                message.content.parts.map(async (part) => {
                  const url = select(part);
                  if (url === undefined) return part;
                  const file = await resolveFile(url as string);
                  return part.type === "file"
                    ? {
                        ...part,
                        data: Buffer.from(file.bytes).toString("base64"),
                      }
                    : part;
                }),
              ),
            },
          })),
        ),
      };
    },
  };
}

it.each([
  [
    "a file part",
    true,
    (resolveFile: MemoryReplayAgentBindings["resolveFile"]) =>
      createAttachmentProcessor(resolveFile),
  ],
  [
    "prompt text",
    false,
    (resolveFile: MemoryReplayAgentBindings["resolveFile"]) =>
      createUrlResolvingProcessor(resolveFile, (part) =>
        part.type === "text"
          ? part.text?.match(/https:\/\/\S+/)?.[0]
          : undefined,
      ),
  ],
  [
    "a URL object",
    true,
    (resolveFile: MemoryReplayAgentBindings["resolveFile"]) =>
      createUrlResolvingProcessor(resolveFile, (part) =>
        part.type === "file" ? new URL(String(part.data)) : undefined,
      ),
  ],
])(
  "answers natively when a processor resolves an undeclared URL from %s",
  async (_, withFilePart, createProcessor) => {
    const nativeFetch = globalThis.fetch;
    const api = installTestApi();
    const apiFetch = globalThis.fetch;
    vi.stubGlobal("fetch", ((
      input: Parameters<typeof fetch>[0],
      init: Parameters<typeof fetch>[1],
    ) =>
      String(input).startsWith("data:")
        ? nativeFetch(input, init)
        : apiFetch(input, init)) as typeof fetch);
    const { store, domain } = await seedAttachmentHistory("", withFilePart);
    const model = new MastraLanguageModelV2Mock({
      provider: "fixture",
      modelId: "actor",
      doStream: async () => textStream("The quote covers two nights."),
    });
    const fetchAttachment = vi.fn(async (_url: string) => ({
      bytes: ATTACHMENT_BYTES,
      mediaType: "application/pdf",
    }));
    const adapter = createMemoryReplayAgent(
      ({ memory, resolveFile }) => ({
        id: "undeclared-processor",
        name: "Undeclared processor",
        instructions: "Answer about the attachment.",
        memory,
        model,
        inputProcessors: [createProcessor(resolveFile)],
      }),
      {
        agentId: AGENT_ID,
        apiUrl: "https://kitaru.invalid",
        apiKey: "fixture",
        requestedModelId: "fixture/actor",
        onRecordingError: () => undefined,
        sourceMemory: () => ({
          settled: async () => {},
          domain,
          configuration: { lastMessages: 20, semanticRecall: false },
          exclusiveAccess: createProcessLocalMemoryAccess(),
        }),
        resolveModel: () => model,
        files: [],
        resolveFile: fetchAttachment,
      },
    );
    try {
      const output = await adapter.stream("What does it cost?", {
        memory: { thread: THREAD, resource: RESOURCE },
      });
      expect(await output.text).toBe("The quote covers two nights.");
      // The application's resolver gets the URL the processor passed, once.
      expect(fetchAttachment).toHaveBeenCalledTimes(1);
      expect(String(fetchAttachment.mock.calls[0]?.[0])).toBe(ATTACHMENT_URL);
      await vi.waitFor(() =>
        expect(
          api.calls.find(
            (call) =>
              call.method === "PATCH" && call.body?.status === "completed",
          )?.body?.metadata,
        ).toMatchObject({
          mastra_replay_state: "ineligible",
          mastra_replay_reason: "file_url_undeclared",
        }),
      );
      expect(JSON.stringify(api.calls)).not.toContain(DOWNLOAD_TOKEN);
    } finally {
      await store.close();
    }
  },
);

/**
 * An observational-memory agent whose declared attachment sits in history.
 *
 * `padding` sets how much history text Mastra observes with it.
 */
async function observedAttachmentAgent(
  supportedUrls: Record<string, RegExp[]>,
  padding: string,
) {
  const nativeFetch = globalThis.fetch;
  const api = installTestApi();
  const apiFetch = globalThis.fetch;
  const downloads: string[] = [];
  vi.stubGlobal("fetch", ((
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ) => {
    const url = String(input);
    if (url.startsWith("data:")) return nativeFetch(input, init);
    if (url.startsWith("https://firebasestorage")) {
      downloads.push(url);
      return Promise.resolve(
        new Response(ATTACHMENT_BYTES, {
          headers: { "content-type": "application/pdf" },
        }),
      );
    }
    if (url.startsWith("kitaru-file:")) downloads.push(url);
    return apiFetch(input, init);
  }) as typeof fetch);
  const { store, domain } = await seedAttachmentHistory(padding);
  let observerCalls = 0;
  const observer = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "observer",
    doStream: async () => {
      observerCalls += 1;
      return textStream(
        "<observations>\nThe user shared a quote for two nights.\n</observations>",
      );
    },
  });
  Object.assign(observer, { supportedUrls });
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: async () => textStream("The quote covers two nights."),
  });
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      semanticRecall: false,
      observationalMemory: {
        scope: "thread",
        observation: {
          model: "fixture/observer",
          messageTokens: 600,
          bufferTokens: false,
        },
        reflection: {
          model: "fixture/reflector",
          observationTokens: 100_000,
        },
      },
    },
  });
  const adapter = createMemoryReplayAgent(
    ({ memory: owned, resolveFile }) => ({
      id: "observed-attachment",
      name: "Observed attachment",
      instructions: "Answer about the attachment.",
      memory: owned,
      model,
      inputProcessors: [createAttachmentProcessor(resolveFile)],
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      apiKey: "fixture",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => memory.settled(),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: (id) =>
        id === "fixture/observer" || id === "fixture/reflector"
          ? observer
          : model,
      files: [ATTACHMENT_URL],
      resolveFile: async () => ({
        bytes: ATTACHMENT_BYTES,
        mediaType: "application/pdf",
      }),
    },
  );
  const closed = (index: number) =>
    api.calls
      .filter(
        (call) =>
          call.method === "PATCH" &&
          call.path.endsWith(`/${api.sessionIds[index]}`) &&
          call.body?.status !== "in_progress",
      )
      .at(-1)?.body;
  /** Record a baseline turn, then replay it, and return both sessions. */
  async function recordAndReplay() {
    const output = await adapter.stream("What does it cost?", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    await vi.waitFor(() =>
      expect(closed(0)?.metadata).toMatchObject({
        mastra_replay_state: "eligible",
      }),
    );
    const baselineObserverCalls = observerCalls;
    downloads.length = 0;
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(closed(0)?.inputs));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(() => expect(closed(1)?.status).toBeDefined());
    return {
      baseline: closed(0),
      baselineObserverCalls,
      downloads,
      replay: closed(1),
      replayObserverCalls: observerCalls - baselineObserverCalls,
    };
  }
  return { recordAndReplay, store };
}

it.each([
  ["reads attachment URLs itself", { "application/pdf": [/^https:\/\//] }],
  ["needs the attachment's bytes", {}],
])(
  "replays a blocking observation over a declared attachment when the observer %s",
  async (_, supportedUrls) => {
    // Enough history that Mastra observes it, attachment included, before the
    // actor's first call.
    const { recordAndReplay, store } = await observedAttachmentAgent(
      supportedUrls,
      " The quote covers two nights at the lake house.".repeat(80),
    );
    try {
      const run = await recordAndReplay();
      expect(run.baselineObserverCalls).toBe(1);
      // The recorded observation answers the call, so neither Mastra nor the
      // provider fetches the captured reference, and its input matches.
      expect(run.replay).toMatchObject({ status: "completed" });
      expect(run.replay?.metadata).toBeUndefined();
      expect(run.downloads).toEqual([]);
      expect(run.replayObserverCalls).toBe(0);
    } finally {
      await store.close();
    }
  },
);

it("counts a declared attachment's tokens in replay as its baseline did", async () => {
  // History just under the observation threshold with the attachment counted
  // from its URL. Counted from its captured reference instead, it would cross
  // the threshold and need an observation production never made.
  const { recordAndReplay, store } = await observedAttachmentAgent(
    { "application/pdf": [/^https:\/\//] },
    " The quote covers two nights at the lake house.".repeat(35),
  );
  try {
    const run = await recordAndReplay();
    expect(run.baselineObserverCalls).toBe(0);
    const inputs = run.baseline?.inputs as
      | { mastra_memory_replay?: { attachmentTokens?: object } }
      | undefined;
    expect(
      Object.keys(inputs?.mastra_memory_replay?.attachmentTokens ?? {}),
    ).toEqual([expect.stringMatching(/^kitaru-file:\/\/sha256\//)]);
    expect(run.replay).toMatchObject({ status: "completed" });
    expect(run.replayObserverCalls).toBe(0);
  } finally {
    await store.close();
  }
});
