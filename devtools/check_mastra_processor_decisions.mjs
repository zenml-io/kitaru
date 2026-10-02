/** Provider-free agent for check_mastra_processor_decisions.py. */
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
const [{ InMemoryStore }, { Memory }] = await Promise.all([
  nativeImport("@mastra/core/storage"),
  nativeImport("@mastra/memory"),
]);
const directory = process.env.CHECK_DIRECTORY;
const replayId = process.env.KITARU_REPLAY_ID;
const report = {
  task_id: process.env.KITARU_TASK_ID,
  replay_id: replayId ?? null,
  classifier_calls: 0,
  actor_calls: 0,
  source_calls: 0,
  actor_inputs: [],
};
assert(process.env.KITARU_API_TOKEN, "Expected the worker's task token");
assert.equal(process.env.KITARU_API_KEY, undefined);
const store = new InMemoryStore();
const memory = new Memory({
  storage: store,
  options: { lastMessages: 10, semanticRecall: false },
});
const domain = store.stores.memory;
assert(domain);
await memory.createThread({
  threadId: "support-thread",
  resourceId: "customer",
});
const classifier = {
  specificationVersion: "v2",
  provider: "fixture",
  modelId: "classifier",
  doGenerate: async () => {
    report.classifier_calls++;
    const skill = (
      await readFile(join(directory, "router.txt"), "utf8")
    ).trim();
    return {
      content: [{ type: "text", text: skill }],
      finishReason: "stop",
      usage: { inputTokens: 7, outputTokens: 3, totalTokens: 10 },
      warnings: [],
    };
  },
};
const actor = {
  specificationVersion: "v2",
  provider: "fixture",
  modelId: "actor",
  supportedUrls: {},
  doStream: async (args) => {
    report.actor_calls++;
    report.actor_inputs.push(JSON.stringify(args.prompt));
    return {
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
          });
          controller.close();
        },
      }),
    };
  },
};
const access = createProcessLocalMemoryAccess();
const adapter = createMemoryReplayAgent(
  ({ memory, decisions }) => {
    const router =
      process.env.CHECK_CAPTURE_DECISION === "0"
        ? undefined
        : decisions.define("skill-router");
    const model = router?.instrumentModel(classifier) ?? classifier;
    const classify = async () => {
      const result = await model.doGenerate({
        prompt: [
          { role: "user", content: [{ type: "text", text: "Pick a skill" }] },
        ],
      });
      return { skills: result.content.map((part) => part.text) };
    };
    return {
      id: "support",
      name: "Support",
      instructions: "Use the selected skill",
      model: actor,
      memory,
      inputProcessors: [
        {
          id: "skill-router",
          async processInput({ messages }) {
            const result = router
              ? await router.run(classify)
              : await classify();
            return messages.map((message) => ({
              ...message,
              content: {
                ...message.content,
                parts: [
                  ...message.content.parts,
                  {
                    type: "text",
                    text: `Selected skills: ${result.skills.join(", ")}`,
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
    agentId: process.env.CHECK_AGENT_ID,
    requestedModelId: "fixture/actor",
    sourceMemory: () => {
      report.source_calls++;
      assert(!replayId, "Replay accessed production memory");
      return {
        domain,
        configuration: memory.getMergedThreadConfig(),
        settled: () => memory.settled(),
        exclusiveAccess: access,
      };
    },
    resolveModel: () => actor,
    costCalculator: ({ requestedModelId }) =>
      requestedModelId === "classifier" ? "0.0005" : "0.001",
  },
);
try {
  const result = await adapter.stream("Please help", {
    memory: { thread: "support-thread", resource: "customer" },
  });
  await result.consumeStream();
  assert.equal(await result.text, "done");
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
    JSON.stringify(report),
  );
}
