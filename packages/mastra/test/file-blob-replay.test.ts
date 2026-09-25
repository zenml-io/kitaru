import type { InputProcessor } from "@mastra/core/processors";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import { createProcessLocalMemoryAccess } from "../src/memory-binding.js";
import {
  createMemoryReplayAgent,
  type MemoryReplayAgentBindings,
} from "../src/stateful-agent.js";
import { textStream } from "./helpers/memory-agent.js";
import { AGENT_ID, installTestApi, REPLAY_ID } from "./helpers.js";

const THREAD = "blob-thread";
const RESOURCE = "blob-resource";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

function attachmentUrl(index: number): string {
  return `https://files.invalid/uploads/scan-${index}.png?alt=media&token=blob-token-${index}`;
}

/** Send each file part's bytes to the model while history keeps the URL. */
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

/** A thread with `textBytes` of text history and one message per attachment. */
async function seedThread(attachments: number, textBytes: number) {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  await domain.saveThread({
    thread: {
      id: THREAD,
      resourceId: RESOURCE,
      title: "Scans",
      createdAt: new Date(1_000),
      updatedAt: new Date(1_000),
      metadata: {},
    },
  });
  const textMessages = 20;
  await domain.saveMessages({
    messages: [
      ...Array.from({ length: textMessages }, (_, index) => ({
        id: `text-message-${index}`,
        threadId: THREAD,
        resourceId: RESOURCE,
        role: "user" as const,
        createdAt: new Date(2_000 + index),
        content: {
          format: 2 as const,
          parts: [
            {
              type: "text" as const,
              text: `${index} ${"x".repeat(Math.floor(textBytes / textMessages))}`,
            },
          ],
        },
      })),
      ...Array.from({ length: attachments }, (_, index) => ({
        id: `scan-message-${index}`,
        threadId: THREAD,
        resourceId: RESOURCE,
        role: "user" as const,
        createdAt: new Date(3_000 + index),
        content: {
          format: 2 as const,
          parts: [
            { type: "text" as const, text: `Scan ${index}` },
            {
              type: "file" as const,
              data: attachmentUrl(index),
              mimeType: "image/png",
            },
          ],
        },
      })),
    ],
  });
  return { store, domain };
}

function fixture(
  domain: NonNullable<InMemoryStore["stores"]["memory"]>,
  fileBytes: (url: string) => Uint8Array,
  declared: string[] = [],
  failBlobUploads = false,
) {
  const nativeFetch = globalThis.fetch;
  const api = installTestApi();
  const apiFetch = globalThis.fetch;
  // Mastra hands base64 file parts to the model as data: URLs it reads back.
  vi.stubGlobal("fetch", ((
    input: Parameters<typeof fetch>[0],
    init: Parameters<typeof fetch>[1],
  ) => {
    if (String(input).startsWith("data:")) return nativeFetch(input, init);
    if (failBlobUploads && new URL(String(input)).pathname === "/api/v1/blobs")
      return Promise.resolve(new Response("unavailable", { status: 503 }));
    return apiFetch(input, init);
  }) as typeof fetch);
  const prompts: string[] = [];
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: async ({ prompt }) => {
      prompts.push(JSON.stringify(prompt));
      return textStream("The scans show the kitchen.");
    },
  });
  const fetchAttachment = vi.fn(async (url: string) => ({
    bytes: fileBytes(url),
    mediaType: "image/png",
  }));
  const adapter = createMemoryReplayAgent(
    ({ memory, resolveFile }) => ({
      id: "blob-files",
      name: "Blob files",
      instructions: "Answer about the scans.",
      memory,
      model,
      inputProcessors: [createAttachmentProcessor(resolveFile)],
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
        configuration: { lastMessages: 60, semanticRecall: false },
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: () => model,
      files: declared,
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
  return { adapter, api, fetchAttachment, final, prompts };
}

it("stores 10 MB of attachments on a 10 MB thread as blobs once and replays them", async () => {
  const { store, domain } = await seedThread(16, 9_500_000);
  // 16 distinct scans of 625 KB each, 10 MB together.
  const scanBytes = (url: string) => {
    const index = Number(/scan-(\d+)/.exec(url)?.[1]);
    return new Uint8Array(625_000).fill(index + 1);
  };
  const { adapter, api, fetchAttachment, final, prompts } = fixture(
    domain,
    scanBytes,
  );
  try {
    for (const [index, text] of ["What is shown?", "Which room?"].entries()) {
      const output = await adapter.stream(text, {
        memory: { thread: THREAD, resource: RESOURCE },
      });
      await output.consumeStream();
      await vi.waitFor(
        () => expect(final(api.sessionIds[index])?.status).toBe("completed"),
        { timeout: 20_000 },
      );
      expect(final(api.sessionIds[index])?.metadata).toMatchObject({
        mastra_replay_state: "eligible",
      });
    }
    // The second turn captured the same files and stored no new bytes.
    expect(api.blobs.size).toBe(16);
    expect(api.blobUploads()).toBe(16);

    const baselineInput = final(api.sessionIds[1])?.inputs as {
      mastra_memory_replay: { files: Record<string, unknown>[] };
    };
    const files = baselineInput.mastra_memory_replay.files;
    expect(files).toHaveLength(16);
    for (const file of files) {
      expect(file).not.toHaveProperty("base64");
      expect(api.blobs.has(String(file.blobId))).toBe(true);
    }
    expect(JSON.stringify(baselineInput).length).toBeLessThan(11_000_000);

    const lastPrompt = prompts.at(-1);
    fetchAttachment.mockClear();
    fetchAttachment.mockRejectedValue(new Error("Signed URL was fetched"));
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baselineInput));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(
      () => expect(final(api.sessionIds[2])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(fetchAttachment).not.toHaveBeenCalled();
    const withoutTimes = (prompt?: string) =>
      prompt?.replace(/"createdAt":\d+/g, "");
    expect(withoutTimes(prompts.at(-1))).toBe(withoutTimes(lastPrompt));
    expect(
      api.calls.filter(
        (call) =>
          call.method === "GET" &&
          /^\/api\/v1\/blobs\/[^/]+\/content$/.test(call.path),
      ),
    ).toHaveLength(16);
  } finally {
    await store.close();
  }
}, 60_000);

it("records and replays a single 9 MB declared attachment", async () => {
  const { store, domain } = await seedThread(0, 0);
  const url = attachmentUrl(0);
  const bytes = new Uint8Array(9 * 1024 * 1024).fill(9);
  const { adapter, api, fetchAttachment, final, prompts } = fixture(
    domain,
    () => bytes,
    [url],
  );
  try {
    const output = await adapter.stream(
      [
        {
          role: "user",
          content: [
            { type: "text", text: "What is in this scan?" },
            { type: "file", data: url, mimeType: "image/png" },
          ],
        },
      ],
      { memory: { thread: THREAD, resource: RESOURCE } },
    );
    await output.consumeStream();
    await vi.waitFor(
      () => expect(final(api.sessionIds[0])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(final(api.sessionIds[0])?.metadata).toMatchObject({
      mastra_replay_state: "eligible",
    });
    expect([...api.blobs.values()].map((blob) => blob.size)).toEqual([
      bytes.byteLength,
    ]);
    const baselineInput = final(api.sessionIds[0])?.inputs;
    fetchAttachment.mockClear();
    fetchAttachment.mockRejectedValue(new Error("Signed URL was fetched"));
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baselineInput));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(
      () => expect(final(api.sessionIds[1])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(fetchAttachment).not.toHaveBeenCalled();
    expect(prompts.at(-1)).toContain(Buffer.from(bytes).toString("base64"));
  } finally {
    await store.close();
  }
}, 60_000);

it("records a turn as ineligible when its files cannot be stored", async () => {
  const { store, domain } = await seedThread(1, 0);
  const { adapter, api, final } = fixture(
    domain,
    () => new Uint8Array([137, 80, 78, 71]),
    [],
    true,
  );
  try {
    const output = await adapter.stream("What is shown?", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    expect(await output.text).toBe("The scans show the kitchen.");
    await vi.waitFor(
      () => expect(final(api.sessionIds[0])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(final(api.sessionIds[0])?.metadata).toMatchObject({
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "file_store_failed",
    });
  } finally {
    await store.close();
  }
}, 60_000);

it("keeps a turn eligible when history holds a captured file inline beyond the replay input budget as base64", async () => {
  const { store, domain } = await seedThread(0, 0);
  const url = attachmentUrl(0);
  // 13 MiB of bytes is over 16 MiB as base64 but within the file limit.
  const bytes = new Uint8Array(13 * 1024 * 1024).fill(7);
  const inline = Buffer.from(bytes).toString("base64");
  await domain.saveMessages({
    messages: [
      {
        id: "inline-scan",
        threadId: THREAD,
        resourceId: RESOURCE,
        role: "user",
        createdAt: new Date(4_000),
        content: {
          format: 2,
          parts: [
            { type: "text", text: "The scan, inline." },
            { type: "file", data: inline, mimeType: "image/png" },
          ],
        },
      },
    ],
  });
  const { adapter, api, fetchAttachment, final, prompts } = fixture(
    domain,
    () => bytes,
    [url],
  );
  try {
    const output = await adapter.stream("What is in the scan?", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await output.consumeStream();
    await vi.waitFor(
      () => expect(final(api.sessionIds[0])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(final(api.sessionIds[0])?.metadata).toMatchObject({
      mastra_replay_state: "eligible",
    });
    const baselineInput = final(api.sessionIds[0])?.inputs;
    expect(JSON.stringify(baselineInput).length).toBeLessThan(100_000);
    fetchAttachment.mockClear();
    fetchAttachment.mockRejectedValue(new Error("Signed URL was fetched"));
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(baselineInput));
    const replay = await adapter.stream("ignored");
    await replay.consumeStream();
    await vi.waitFor(
      () => expect(final(api.sessionIds[1])?.status).toBe("completed"),
      { timeout: 20_000 },
    );
    expect(fetchAttachment).not.toHaveBeenCalled();
    expect(prompts.at(-1)).toContain(inline);
  } finally {
    await store.close();
  }
}, 60_000);
