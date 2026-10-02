/** Provider-free agent entrypoint for check_mastra_memory_replay.py. */
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../packages/mastra/dist/memory.js";

const require = createRequire(
  new URL("../packages/mastra/package.json", import.meta.url),
);
const nativeImport = (name) =>
  import(pathToFileURL(require.resolve(name).replace(/\.cjs$/, ".js")).href);
const [{ InMemoryStore }, { Memory }, { z }] = await Promise.all([
  nativeImport("@mastra/core/storage"),
  nativeImport("@mastra/memory"),
  nativeImport("zod/v4"),
]);
const directory = process.env.CHECK_DIRECTORY;
const replayId = process.env.KITARU_REPLAY_ID;
const thread = "historical-thread";
const resource = "historical-resource";
const fileUrl = "https://files.invalid/historical.pdf?token=historical-secret";
const bytes = new Uint8Array(40000).fill(65);
const report = {
  task_id: process.env.KITARU_TASK_ID,
  replay_id: replayId ?? null,
  task_inputs_in_environment: process.env.KITARU_TASK_INPUTS !== undefined,
  source_calls: 0,
  file_calls: 0,
  dynamic_calls: 0,
  processor_calls: 0,
  actor_calls: 0,
};
assert(process.env.KITARU_API_TOKEN, "Expected the ordinary worker task token");
assert.equal(
  process.env.KITARU_API_KEY,
  undefined,
  "Administrative key leaked to task",
);
const denied = await fetch(
  `${process.env.KITARU_API_URL}/api/v1/blobs/${process.env.CHECK_UNRELATED_BLOB}/content`,
  {
    headers: { Authorization: `Bearer ${process.env.KITARU_API_TOKEN}` },
  },
);
assert.equal(denied.status, 403, "Task unexpectedly read an unrelated blob");
report.unrelated_blob_status = denied.status;
const stream = (parts, finishReason = "stop") => ({
  stream: new ReadableStream({
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
});
const text = (value) =>
  stream([
    { type: "text-start", id: "text" },
    { type: "text-delta", id: "text", delta: value },
    { type: "text-end", id: "text" },
  ]);
const actor = (id) => ({
  specificationVersion: "v2",
  supportedUrls: {},
  doGenerate: async () => {
    throw new Error("Unexpected generate call");
  },
  provider: "fixture",
  modelId: id,
  doStream: async (args) => {
    report.actor_calls++;
    const prompt = JSON.stringify(args.prompt);
    assert(prompt.includes("historical-blue"));
    assert(prompt.includes("Extra context"));
    assert(prompt.includes("HISTORICAL_SKILL"));
    assert(!prompt.includes("production-today"));
    if (replayId) {
      assert(prompt.includes("Replay application instruction"));
      assert(!prompt.includes("Original application instruction"));
      assert.equal(id, "replacement");
    }
    if (report.actor_calls === 1)
      return stream(
        [
          {
            type: "tool-call",
            toolCallId: "change-preference",
            toolName: "updateWorkingMemory",
            input: JSON.stringify({
              memory: {
                preference: replayId ? "replay-green" : "baseline-red",
              },
            }),
          },
        ],
        "tool-calls",
      );
    assert(prompt.includes(replayId ? "replay-green" : "baseline-red"));
    return text("done");
  },
});
const observationModel = (id) => ({
  specificationVersion: "v2",
  supportedUrls: {},
  provider: "fixture",
  modelId: id,
  doGenerate: async () => {
    throw new Error("Unexpected OM generate call");
  },
  doStream: async () => {
    throw new Error("Unexpected OM model call");
  },
});
const models = {
  actor: actor("actor"),
  replacement: actor("replacement"),
  observer: observationModel("observer"),
  reflector: observationModel("reflector"),
};
const store = new InMemoryStore();
const memory = new Memory({
  storage: store,
  options: {
    lastMessages: 10,
    semanticRecall: false,
    workingMemory: {
      enabled: true,
      scope: "thread",
      schema: z.object({ preference: z.string() }),
    },
    observationalMemory: {
      scope: "thread",
      observation: { model: "fixture/observer", messageTokens: 100000 },
      reflection: { model: "fixture/reflector", observationTokens: 100000 },
    },
  },
});
const domain = store.stores.memory;
assert(domain);
await memory.createThread({
  threadId: thread,
  resourceId: resource,
  title: "Historical thread",
});
const currentProduction = JSON.parse(
  await readFile(join(directory, "production.json"), "utf8"),
);
await memory.updateWorkingMemory({
  threadId: thread,
  resourceId: resource,
  workingMemory: JSON.stringify(currentProduction),
});
const nestedItems = Array.from({ length: 1500 }, (_, item) => ({
  id: item,
  details: Object.fromEntries(
    Array.from({ length: 10 }, (_, field) => [`field${field}`, field]),
  ),
}));
await domain.saveMessages({
  messages: Array.from({ length: 830 }, (_, index) => ({
    id: `historical-${index}`,
    role: "user",
    content: {
      format: 2,
      parts: [
        {
          type: "text",
          text: index === 0 ? "HISTORICAL_MESSAGE: " + "x".repeat(1_100_000) : `history ${index}`,
        },
      ],
      ...(index === 0 ? { metadata: { items: nestedItems } } : {}),
    },
    createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, index)),
    threadId: thread,
    resourceId: resource,
  })),
});
const omRecord = await domain.initializeObservationalMemory({
  threadId: thread,
  resourceId: resource,
  scope: "thread",
  config: { fixture: true },
});
await domain.updateActiveObservations({
  id: omRecord.id,
  observations: "HISTORICAL_OBSERVATION: likes blue.",
  tokenCount: 10,
  lastObservedAt: new Date(Date.UTC(2026, 0, 1, 0, 0, 0)),
  observedMessageIds: ["historical-0"],
});
const sourceDomain = new Proxy(domain, {
  get(target, key) {
    const value = Reflect.get(target, key, target);
    if (typeof value !== "function") return value;
    return (...args) => {
      if (replayId)
        throw new Error(
          `Production memory accessed during replay: ${String(key)}`,
        );
      return Reflect.apply(value, target, args);
    };
  },
});
const dynamic = (value) => () => {
  if (replayId)
    throw new Error("Live dynamic configuration was resolved during replay");
  report.dynamic_calls++;
  return value;
};
const agent = createMemoryReplayAgent(
  ({ memory: isolated, resolveFile, workspace }) => ({
    id: "headless-memory",
    name: "Headless memory",
    memory: isolated,
    workspace,
    instructions: dynamic("Original application instruction"),
    model: dynamic(models.actor),
    defaultOptions: dynamic({ maxSteps: 3 }),
    inputProcessors: [
      {
        id: "historical-file",
        async processInput({ messages }) {
          report.processor_calls++;
          const filePart = messages
            .flatMap((message) => message.content.parts)
            .find((part) => part.type === "file");
          assert(filePart, "Expected a file in native processor input");
          const file = await resolveFile(String(filePart.data));
          assert.deepEqual(file.bytes, bytes);
          return messages.map((message) => ({
            ...message,
            content: {
              ...message.content,
              parts: message.content.parts.map((part) =>
                part.type === "file"
                  ? {
                      ...part,
                      data: Buffer.from(file.bytes).toString("base64"),
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
    agentId: process.env.CHECK_AGENT_ID,
    requestedModelId: "fixture/actor",
    allowedReplayModels: ["fixture/replacement"],
    sourceMemory: () => {
      report.source_calls++;
      if (replayId)
        throw new Error("Production source requested during replay");
      return {
        domain: sourceDomain,
        configuration: memory.getMergedThreadConfig(),
        settled: () => memory.settled(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      };
    },
    resolveModel: (id) => {
      const model = models[id.split("/").at(-1)];
      assert(model, `Unexpected model ${id}`);
      return model;
    },
    files: [fileUrl],
    resolveFile: async () => {
      report.file_calls++;
      if (replayId) throw new Error("Live file requested during replay");
      return { bytes, mediaType: "application/pdf" };
    },
    skillsDirectory: join(directory, "skills"),
  },
);
try {
  const result = await agent.stream(
    [
      {
        role: "user",
        content: [
          { type: "text", text: "Update the preference using the attachment." },
          { type: "file", data: new URL(fileUrl), mimeType: "application/pdf" },
        ],
      },
    ],
    {
      memory: { thread, resource },
      context: [{ role: "system", content: "Extra context" }],
    },
  );
  await result.consumeStream();
  assert.equal(await result.text, "done");
  assert.equal(report.processor_calls, 1);
  assert.equal(report.actor_calls, 2);
  assert.equal(report.dynamic_calls, replayId ? 0 : 3);
  assert.equal(report.source_calls, replayId ? 0 : 1);
  assert.equal(report.file_calls, replayId ? 0 : 1);
  if (replayId) {
    assert.equal(report.task_inputs_in_environment, false);
    assert.deepEqual(
      JSON.parse(await readFile(join(directory, "production.json"), "utf8")),
      { preference: "production-today" },
    );
    assert(
      (
        await memory.getWorkingMemory({
          threadId: thread,
          resourceId: resource,
        })
      ).includes("production-today"),
    );
  }
  report.result = "passed";
} catch (error) {
  report.result = "failed";
  report.error = String(error);
  throw error;
} finally {
  await memory.settled();
  await store.close();
  await writeFile(
    join(directory, `${report.task_id}.json`),
    JSON.stringify(report, null, 2),
  );
}
