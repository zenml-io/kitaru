import type { InputProcessor } from "@mastra/core/processors";
import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
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
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  // History as an earlier turn left it: the attachment's signed URL, not bytes.
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
            { type: "text", text: `Here is my quote: ${ATTACHMENT_URL}` },
            { type: "file", data: ATTACHMENT_URL, mimeType: "application/pdf" },
          ],
        },
      },
    ],
  });
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
