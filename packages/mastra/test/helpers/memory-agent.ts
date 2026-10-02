import { Agent } from "@mastra/core/agent";
import { ModelRouterLanguageModel } from "@mastra/core/llm";
import type { InputProcessor } from "@mastra/core/processors";
import { RequestContext } from "@mastra/core/request-context";
import { InMemoryStore, type MemoryStorage } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { LocalSkillSource, Workspace } from "@mastra/core/workspace";
import { Memory } from "@mastra/memory";
import { type Mock, vi } from "vitest";
import { z } from "zod/v4";

export const THREAD = "historical-thread";
export const RESOURCE = "historical-resource";
export const FILE_URL = "https://files.invalid/report.pdf";
export const FILE_BYTES = new Uint8Array([37, 80, 68, 70, 45, 49, 0, 255]);
export type ModelCall = Parameters<MastraLanguageModelV2Mock["doStream"]>[0];
type StreamPart =
  Awaited<
    ReturnType<MastraLanguageModelV2Mock["doStream"]>
  >["stream"] extends ReadableStream<infer T>
    ? T
    : never;

export function textStream(
  text: string,
): Awaited<ReturnType<MastraLanguageModelV2Mock["doStream"]>> {
  return streamParts([
    { type: "text-start", id: "text" },
    { type: "text-delta", id: "text", delta: text },
    { type: "text-end", id: "text" },
  ]);
}

export function streamParts(
  parts: StreamPart[],
  finishReason: "stop" | "tool-calls" = "stop",
): Awaited<ReturnType<MastraLanguageModelV2Mock["doStream"]>> {
  return {
    stream: new ReadableStream<StreamPart>({
      start(controller) {
        controller.enqueue({ type: "stream-start", warnings: [] });
        for (const part of parts) controller.enqueue(part);
        controller.enqueue({
          type: "finish",
          finishReason,
          usage: { inputTokens: 5, outputTokens: 5, totalTokens: 10 },
        });
        controller.close();
      },
    }),
  };
}

export function memoryModel(
  kind: "observer" | "reflector",
  wait?: () => Promise<void>,
): { calls: ModelCall[]; model: MastraLanguageModelV2Mock } {
  const calls: ModelCall[] = [];
  return {
    calls,
    model: new MastraLanguageModelV2Mock({
      modelId: kind,
      provider: "fixture",
      doStream: async (options) => {
        calls.push(options);
        await wait?.();
        const observation =
          kind === "observer"
            ? `OBSERVED_REPLAY: ${"The user changed the preference to replay-green. ".repeat(15)}`
            : "REFLECTED_REPLAY: user prefers replay-green.";
        return textStream(
          `<observations>\n${observation}\n</observations>\n<current-task>Continue.</current-task>`,
        );
      },
    }),
  };
}

/** Run one short native Mastra turn and return the OM configuration it stores. */
export async function getNativeOMRecordConfig(
  options: NonNullable<ConstructorParameters<typeof Memory>[0]>["options"],
  prepare?: (domain: MemoryStorage) => void,
): Promise<unknown> {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  prepare?.(domain);
  const agent = new Agent({
    id: "native-om-record",
    name: "Native OM record",
    instructions: "Answer briefly.",
    model: new MastraLanguageModelV2Mock({
      modelId: "actor",
      provider: "fixture",
      doStream: async () => textStream("noted"),
    }),
    memory: new Memory({ storage: store, options }),
  });
  try {
    const result = await agent.stream("Hello.", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    await result.consumeStream();
    const [row] = await domain.getObservationalMemoryHistory(THREAD, RESOURCE);
    if (!row) throw new Error("Native Mastra did not create an OM record");
    return row.config;
  } finally {
    await store.close();
  }
}

// Default continuation hints put Mastra's built-in extractors into the record.
export const TRIP_OM_OPTIONS = {
  lastMessages: 20,
  observationalMemory: {
    observation: {
      model: "fixture/observer",
      messageTokens: 300,
      bufferTokens: 0.2,
      bufferActivation: 1,
      blockAfter: 1.1,
    },
    reflection: {
      model: "fixture/reflector",
      observationTokens: 200,
      bufferActivation: 1,
    },
  },
};

/** An OM model that observes spring-trip plans, as a mock or a model-router model. */
export function createTripOMModel(
  kind: "observer" | "reflector",
  router: boolean,
): {
  doStream: Mock<() => Promise<ReturnType<typeof textStream>>>;
  model: MastraLanguageModelV2Mock | ModelRouterLanguageModel;
} {
  const doStream = vi.fn(async () =>
    textStream(
      kind === "observer"
        ? `<observations>\n${"The user keeps sharing travel plans for the spring trip. ".repeat(12)}\n</observations>\n<current-task>Continue.</current-task>`
        : "<observations>\nREFLECTED: the user is planning a spring trip.\n</observations>",
    ),
  );
  if (!router)
    return {
      doStream,
      model: new MastraLanguageModelV2Mock({
        modelId: kind,
        provider: "fixture",
        doStream,
      }),
    };
  // The router model carries the gateway catalog as enumerable state.
  const model = new ModelRouterLanguageModel("openai/gpt-5-nano");
  Object.assign(model, { doStream });
  return { doStream, model };
}

let bufferingOps: Map<string, Promise<void>> | undefined;

/**
 * Wait, up to `waitMs`, for buffered observational-memory work that earlier
 * turns left running in Mastra's process-wide buffering map. Tests share one
 * thread id, and a recorded turn does not start eligible while such work runs.
 */
export async function settleBuffering(waitMs = 5_000): Promise<void> {
  const deadline = Date.now() + waitMs;
  while (bufferingOps?.size && Date.now() < deadline)
    await Promise.race([
      Promise.allSettled([...bufferingOps.values()]),
      new Promise((resolve) => setTimeout(resolve, deadline - Date.now())),
    ]);
}

export function createMemoryRuntime(
  options: { observerWait?: () => Promise<void>; messageTokens?: number } = {},
) {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing native memory domain");
  const observer = memoryModel("observer", options.observerWait);
  const reflector = memoryModel("reflector");
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      semanticRecall: false,
      workingMemory: {
        enabled: true,
        scope: "thread",
        schema: z.object({
          preference: z.string(),
          notes: z.string().optional(),
        }),
      },
      observationalMemory: {
        scope: "thread",
        observation: {
          model: observer.model,
          messageTokens: options.messageTokens ?? 300,
          bufferTokens: 0.2,
          bufferActivation: 1,
          blockAfter: 1.1,
          bufferOnIdle: true,
        },
        reflection: {
          model: reflector.model,
          observationTokens: 100,
          bufferActivation: 0.5,
          blockAfter: 1.1,
        },
      },
    },
  });
  void memory.omEngine.then((engine) => {
    bufferingOps ??= (
      engine?.buffering.constructor as
        | { asyncBufferingOps?: Map<string, Promise<void>> }
        | undefined
    )?.asyncBufferingOps;
  });
  return { store, domain, memory, observer, reflector };
}
export type MemoryRuntime = ReturnType<typeof createMemoryRuntime>;

export async function seedMemory(runtime: MemoryRuntime) {
  await runtime.memory.createThread({
    threadId: THREAD,
    resourceId: RESOURCE,
    title: "Historical conversation",
  });
  await runtime.domain.saveResource({
    resource: {
      id: RESOURCE,
      createdAt: new Date(0),
      updatedAt: new Date(0),
      metadata: { tenant: "fixture" },
    },
  });
  await runtime.memory.updateWorkingMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    workingMemory: JSON.stringify({ preference: "historical-blue" }),
  });
  await runtime.memory.saveMessages({
    messages: [
      {
        id: "historical-message",
        role: "user",
        content: {
          format: 2,
          parts: [{ type: "text", text: "HISTORICAL_MESSAGE: remember blue." }],
        },
        createdAt: new Date("2026-01-01T00:00:00Z"),
        threadId: THREAD,
        resourceId: RESOURCE,
      },
    ],
  });
  const record = await runtime.domain.initializeObservationalMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    scope: "thread",
    config: { fixture: true },
  });
  await runtime.domain.updateActiveObservations({
    id: record.id,
    observations: "HISTORICAL_OBSERVATION: likes blue.",
    tokenCount: 10,
    lastObservedAt: new Date("2025-12-31T00:00:00Z"),
    observedMessageIds: [],
  });
}

// The caller must hold exclusive application-level access across settled() and
// these reads. settled() joins this instance's jobs; it is not a storage lock.
export async function snapshotMemory(
  runtime: MemoryRuntime,
  exclusive: boolean,
) {
  if (!exclusive) throw new Error("Exclusive source-thread ownership required");
  await runtime.memory.settled();
  const thread = await runtime.domain.getThreadById({ threadId: THREAD });
  const resource = await runtime.domain.getResourceById({
    resourceId: RESOURCE,
  });
  const { messages } = await runtime.domain.listMessages({
    threadId: THREAD,
    perPage: false,
  });
  const records = await runtime.domain.getObservationalMemoryHistory(
    THREAD,
    RESOURCE,
  );
  if (
    records.some(
      (record) =>
        record.isObserving ||
        record.isReflecting ||
        record.isBufferingObservation ||
        record.isBufferingReflection,
    )
  ) {
    throw new Error("Unjoined observational-memory work");
  }
  return structuredClone({ thread, resource, messages, records });
}

export async function restoreMemory(
  snapshot: Awaited<ReturnType<typeof snapshotMemory>>,
  runtime: MemoryRuntime,
) {
  const copy = structuredClone(snapshot);
  if (copy.thread)
    await runtime.domain.saveThread({ thread: structuredClone(copy.thread) });
  if (copy.resource)
    await runtime.domain.saveResource({ resource: copy.resource });
  await runtime.domain.saveMessages({ messages: copy.messages });
  // saveMessages advances thread.updatedAt; restore the historical record last.
  if (copy.thread)
    await runtime.domain.saveThread({ thread: structuredClone(copy.thread) });
  for (const record of copy.records)
    await runtime.domain.insertObservationalMemoryRecord(record);
}

export function makeMemoryAgent(options: {
  runtime: MemoryRuntime;
  model: MastraLanguageModelV2Mock;
  skillsPath: string;
  resolveFile: (url: string) => Promise<Uint8Array>;
  processorRuns: string[];
  configurationCalls?: string[];
  processors?: InputProcessor[];
}) {
  const context = new RequestContext<{
    applicationInstructions: string;
    temperature: number;
  }>();
  context.set(
    "applicationInstructions",
    "APPLICATION_INSTRUCTIONS: answer using memory.",
  );
  context.set("temperature", 0.25);
  const workspace = new Workspace({
    skills: ["skills"],
    skillSource: new LocalSkillSource({ basePath: options.skillsPath }),
  });
  const processor: InputProcessor = {
    id: "bound-file-resolver",
    async processInput({ messages }) {
      options.processorRuns.push(FILE_URL);
      const bytes = await options.resolveFile(FILE_URL);
      return messages.map((message) => ({
        ...message,
        content: {
          ...message.content,
          parts: message.content.parts.map((part) =>
            part.type === "file" && part.data === FILE_URL
              ? {
                  ...part,
                  data: `data:application/pdf;base64,${Buffer.from(bytes).toString("base64")}`,
                }
              : part,
          ),
        },
      }));
    },
  };
  const agent = new Agent({
    id: "native-memory-proof",
    name: "Native memory proof",
    instructions: ({ requestContext }) => {
      options.configurationCalls?.push("instructions");
      return String(requestContext.get("applicationInstructions"));
    },
    model: () => {
      options.configurationCalls?.push("model");
      return options.model;
    },
    defaultOptions: ({ requestContext }) => {
      options.configurationCalls?.push("defaultOptions");
      return {
        maxSteps: 5,
        modelSettings: {
          temperature: Number(requestContext.get("temperature")),
        },
      };
    },
    memory: () => options.runtime.memory,
    tools: {
      readConversationEvidence: createTool({
        id: "readConversationEvidence",
        description: "Read recorded conversation evidence",
        inputSchema: z.object({}),
        execute: async () => {
          await options.runtime.memory.settled();
          return {
            evidence: "The replay user now prefers green. ".repeat(400),
          };
        },
      }),
    },
    workspace,
    inputProcessors: [processor, ...(options.processors ?? [])],
  });
  return { agent, context, workspace };
}

export async function consumeMemoryAgent(
  fixture: ReturnType<typeof makeMemoryAgent>,
  text = "Use the attachment.",
) {
  const output = await fixture.agent.stream(
    [
      {
        role: "user",
        content: [
          { type: "text", text },
          {
            type: "file",
            data: new URL(FILE_URL),
            mimeType: "application/pdf",
          },
        ],
      },
    ],
    {
      memory: { thread: THREAD, resource: RESOURCE },
      requestContext: fixture.context,
      system: "EXTRA_SYSTEM_CONTEXT: preserve this contribution.",
    },
  );
  let result = "";
  for await (const chunk of output.textStream) result += chunk;
  return { output, text: result };
}
