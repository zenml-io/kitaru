import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { loadTypescriptPackageMetadata } from "./typescript-packages.mjs";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const lowerMastraVersion = "1.51.0";
const upperMastraVersion = "1.67.0";

function parseOutputDirectory(args) {
  if (args.length === 0) {
    return undefined;
  }
  if (args.length !== 2 || args[0] !== "--output-dir") {
    throw new Error("Usage: smoke-typescript-packages.mjs [--output-dir PATH]");
  }
  return resolve(repositoryRoot, args[1]);
}

function run(command, args, cwd = repositoryRoot) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit" });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} exited with ${result.status}`);
  }
}

function assertPackageContents(tarball) {
  const result = spawnSync("tar", ["-tzf", tarball], { encoding: "utf8" });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`tar -tzf ${tarball} exited with ${result.status}`);
  }
  if (!result.stdout.split("\n").includes("package/LICENSE")) {
    throw new Error(`${tarball} does not contain package/LICENSE`);
  }
}

function writeConsumerFiles(consumerRoot) {
  writeFileSync(
    join(consumerRoot, "package.json"),
    JSON.stringify({ name: "kitaru-package-smoke", private: true, type: "module" }),
  );
  writeFileSync(
    join(consumerRoot, "index.mjs"),
    `import { KitaruClient } from "@zenml-io/kitaru";
import { createKitaruClient } from "@zenml-io/kitaru/node";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";
import { createKitaruGenerateText, createKitaruToolLoopAgent } from "@zenml-io/kitaru-vercel-ai";
if (![KitaruClient, createKitaruClient, KitaruAgent, createKitaruGenerateText, createKitaruToolLoopAgent].every(Boolean)) {
  throw new Error("Packed package exports are missing");
}
`,
  );
  writeFileSync(
    join(consumerRoot, "packages.ts"),
    `import type { Agent } from "@mastra/core/agent";
import { KitaruClient, type KitaruEnvironmentVariables } from "@zenml-io/kitaru";
import { createKitaruClient } from "@zenml-io/kitaru/node";
import { KitaruAgent, type KitaruAgentOptions } from "@zenml-io/kitaru-mastra";
import { createKitaruGenerateText, createKitaruToolLoopAgent, type KitaruToolLoopAgentSettings, type KitaruVercelAIOptions } from "@zenml-io/kitaru-vercel-ai";
const environment: KitaruEnvironmentVariables = { KITARU_API_URL: "http://localhost" };
new KitaruClient({ apiUrl: environment.KITARU_API_URL });
void createKitaruClient({ apiKey: "package-smoke", apiUrl: environment.KITARU_API_URL });
const mastraOptions: KitaruAgentOptions = {
  agentId: "package-smoke",
  requestedModelId: "package-smoke-model",
};
declare const mastraAgent: Agent;
new KitaruAgent(mastraAgent, mastraOptions);
new KitaruAgent({ generate: async () => ({}) }, mastraOptions);
const vercelOptions: KitaruVercelAIOptions = { agentId: "package-smoke" };
createKitaruGenerateText(vercelOptions);
declare const agentSettings: KitaruToolLoopAgentSettings;
createKitaruToolLoopAgent(agentSettings, vercelOptions);
`,
  );
  writeFileSync(
    join(consumerRoot, "generate.mjs"),
    `import { Agent } from "@mastra/core/agent";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";

const sessionId = "018f0000-0000-7000-8000-000000000100";
const calls = [];
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(String(input));
  const method = init.method ?? "GET";
  const body = init.body ? JSON.parse(String(init.body)) : undefined;
  calls.push({ body, method, path: url.pathname });
  if (method === "POST" && url.pathname === "/api/v1/sessions") {
    return Response.json(
      { id: sessionId, origin: "recorded", status: "in_progress" },
      { status: 201 },
    );
  }
  if (method === "POST" && url.pathname.endsWith("/nodes")) {
    return Response.json([], { status: 200 });
  }
  if (method === "PATCH" && url.pathname === "/api/v1/sessions/" + sessionId) {
    return Response.json({
      id: sessionId,
      origin: "recorded",
      status: body.status,
    });
  }
  throw new Error("Unexpected package smoke request: " + method + " " + url.pathname);
};

const model = {
  doGenerate: async () => ({
    content: [{ text: "recorded package smoke", type: "text" }],
    finishReason: "stop",
    request: { body: { prompt: "package smoke" } },
    response: { id: "package-smoke-response", modelId: "package-smoke-model" },
    usage: { inputTokens: 1, outputTokens: 2, totalTokens: 3 },
    warnings: [],
  }),
  doStream: async () => {
    throw new Error("Package smoke model does not stream");
  },
  modelId: "package-smoke-model",
  provider: "package-smoke",
  specificationVersion: "v2",
  supportedUrls: {},
};
const agent = new Agent({
  id: "package-smoke-agent",
  instructions: "Respond deterministically.",
  model,
  name: "Package smoke agent",
});
const recorded = new KitaruAgent(agent, {
  agentId: sessionId,
  apiUrl: "https://api.example",
  requestedModelId: "package-smoke-model",
});
const result = await recorded.generate("hello");
if (result.text !== "recorded package smoke") {
  throw new Error("Unexpected Mastra result: " + result.text);
}
const recordedNodes = calls
  .filter((call) => call.method === "POST" && call.path.endsWith("/nodes"))
  .flatMap((call) => call.body.nodes);
if (!recordedNodes.some((node) => node.node_type === "llm_call")) {
  throw new Error("Deterministic generation omitted its LLM node");
}
const completion = calls.find(
  (call) =>
    call.method === "PATCH" &&
    call.path === "/api/v1/sessions/" + sessionId,
);
if (
  completion?.body.status !== "completed" ||
  completion.body.outputs?.text !== "recorded package smoke"
) {
  throw new Error("Deterministic generation did not record completed output");
}
`,
  );
  writeFileSync(
    join(consumerRoot, "stream.mjs"),
    `import { Agent } from "@mastra/core/agent";
import { createTool } from "@mastra/core/tools";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";
import { z } from "zod";

const mastraVersion = process.argv[2];
const sessionId = "018f0000-0000-7000-8000-000000000101";
const replayId = "018f0000-0000-7000-8000-000000000102";
const calls = [];
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(String(input));
  const method = init.method ?? "GET";
  const body = init.body ? JSON.parse(String(init.body)) : undefined;
  calls.push({ body, method, path: url.pathname });
  if (method === "POST" && url.pathname === "/api/v1/sessions") {
    return Response.json(
      { id: sessionId, origin: "recorded", status: "in_progress" },
      { status: 201 },
    );
  }
  if (method === "POST" && url.pathname.endsWith("/nodes")) {
    return Response.json([], { status: 200 });
  }
  if (method === "PATCH" && url.pathname === "/api/v1/sessions/" + sessionId) {
    return Response.json({
      id: sessionId,
      origin: "recorded",
      status: body.status,
    });
  }
  if (method === "GET" && url.pathname === "/api/v1/replays/" + replayId) {
    return Response.json({
      baseline_session_id: sessionId,
      id: replayId,
      job_id: "018f0000-0000-7000-8000-000000000104",
      override: null,
      status: "pending",
      tool_policy: {
        default: { on_miss: "fail", scope: "baseline", type: "history" },
        tools: {},
      },
    });
  }
  if (method === "POST" && url.pathname.endsWith("/tool-lookup")) {
    return Response.json({
      match: { error: null, result: { forecast: "sunny", city: "Amsterdam" }, status: "completed" },
      rejected_candidate: false,
    });
  }
  throw new Error("Unexpected stream smoke request: " + method + " " + url.pathname);
};

let modelCalls = 0;
let toolCalls = 0;
const model = {
  doGenerate: async () => {
    throw new Error("Stream smoke must not generate");
  },
  doStream: async () => {
    modelCalls += 1;
    const chunks = modelCalls % 2 === 1
      ? [
          { type: "stream-start", warnings: [] },
          { id: "tool-response", modelId: "served-stream-model", type: "response-metadata" },
          {
            input: '{"city":"Amsterdam"}',
            toolCallId: "call-weather",
            toolName: "weather",
            type: "tool-call",
          },
          {
            finishReason: "tool-calls",
            type: "finish",
            usage: { inputTokens: 4, outputTokens: 2, totalTokens: 6 },
          },
        ]
      : [
          { type: "stream-start", warnings: [] },
          { id: "text-response", modelId: "served-stream-model", type: "response-metadata" },
          { id: "answer", type: "text-start" },
          { delta: "sunny ", id: "answer", type: "text-delta" },
          { delta: "today", id: "answer", type: "text-delta" },
          { id: "answer", type: "text-end" },
          {
            finishReason: "stop",
            type: "finish",
            usage: { inputTokens: 3, outputTokens: 2, totalTokens: 5 },
          },
        ];
    return {
      stream: new ReadableStream({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(chunk);
          controller.close();
        },
      }),
    };
  },
  modelId: "stream-smoke-model",
  provider: "stream-smoke",
  specificationVersion: "v2",
  supportedUrls: {},
};
const weather = createTool({
  id: "weather",
  description: "Return deterministic weather",
  inputSchema: z.object({ city: z.string() }),
  execute: async ({ city }) => {
    toolCalls += 1;
    return { forecast: "sunny", city };
  },
});
const agent = new Agent({
  id: "stream-smoke-agent",
  instructions: "Use weather.",
  model,
  name: "Stream smoke agent",
  tools: { weather },
});
const recorded = new KitaruAgent(agent, {
  agentId: sessionId,
  apiUrl: "https://api.example",
  requestedModelId: "stream-smoke-model",
});

if (mastraVersion === "1.51.0") {
  await recorded.stream("weather").then(
    () => {
      throw new Error("Mastra 1.51 stream unexpectedly started");
    },
    (error) => {
      if (!String(error).includes("requires a stable @mastra/core 1.67.x")) {
        throw error;
      }
    },
  );
  if (calls.length !== 0 || modelCalls !== 0 || toolCalls !== 0) {
    throw new Error("Mastra 1.51 stream rejection caused side effects");
  }
} else {
  const output = await recorded.stream("weather");
  const chunks = [];
  for await (const chunk of output.textStream) chunks.push(chunk);
  if (chunks.join("") !== "sunny today" || chunks.length !== 2) {
    throw new Error("Unexpected native stream chunks: " + JSON.stringify(chunks));
  }
  if (modelCalls !== 2 || toolCalls !== 1) {
    throw new Error("Deterministic stream did not execute two model steps and one tool");
  }
  const nodes = calls
    .filter((call) => call.method === "POST" && call.path.endsWith("/nodes"))
    .flatMap((call) => call.body.nodes);
  if (nodes.filter((node) => node.node_type === "llm_call").length !== 2) {
    throw new Error("Packaged stream omitted its two LLM nodes");
  }
  const tool = nodes.find((node) => node.node_type === "tool_call");
  if (tool?.outputs?.forecast !== "sunny" || tool?.status !== "completed") {
    throw new Error("Packaged stream omitted its completed tool result");
  }
  const completion = calls.find(
    (call) => call.method === "PATCH" && call.path.endsWith(sessionId),
  );
  if (
    completion?.body.status !== "completed" ||
    completion.body.outputs?.text !== "sunny today" ||
    completion.body.outputs?.step_count !== 2
  ) {
    throw new Error("Packaged stream did not record its final output");
  }

  process.env.KITARU_REPLAY_ID = replayId;
  process.env.KITARU_TASK_INPUTS = JSON.stringify("recorded weather");
  const replay = await recorded.stream("caller weather");
  const replayChunks = [];
  for await (const chunk of replay.textStream) replayChunks.push(chunk);
  delete process.env.KITARU_REPLAY_ID;
  delete process.env.KITARU_TASK_INPUTS;
  if (replayChunks.join("") !== "sunny today" || modelCalls !== 4 || toolCalls !== 1) {
    throw new Error("Packaged stream replay did not mock the history tool");
  }
  const replaySession = calls.find(
    (call) => call.method === "POST" && call.path === "/api/v1/sessions" && call.body.origin === "replay",
  );
  if (replaySession?.body.inputs !== "recorded weather") {
    throw new Error("Packaged stream replay did not use the recorded input");
  }
}
`,
  );
  writeFileSync(
    join(consumerRoot, "stream.ts"),
    `import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";
import { z } from "zod";

const agent = new Agent({
  id: "typed-stream",
  instructions: "Respond.",
  model: new MastraLanguageModelV2Mock({
    modelId: "typed-model",
    provider: "package-smoke",
  }),
  name: "Typed stream",
});
const recorded = new KitaruAgent(agent, {
  agentId: "018f0000-0000-7000-8000-000000000103",
  apiUrl: "https://api.example",
  requestedModelId: "typed-model",
});
const wrapperAsNative: typeof agent.stream = recorded.stream;
const nativeAsWrapper: typeof recorded.stream = agent.stream;
void wrapperAsNative;
void nativeAsWrapper;

async function assertSchemaInference(): Promise<void> {
  const output = await recorded.stream("hello", {
    structuredOutput: { schema: z.object({ answer: z.string() }) },
  });
  const value: { answer: string } = await output.object;
  void value;
}
void assertSchemaInference;
`,
  );
  writeFileSync(
    join(consumerRoot, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        lib: ["ES2022", "DOM"],
        module: "NodeNext",
        moduleResolution: "NodeNext",
        noEmit: true,
        skipLibCheck: true,
        strict: true,
        types: [],
      },
      include: ["packages.ts"],
    }),
  );
  writeFileSync(
    join(consumerRoot, "tsconfig.stream.json"),
    JSON.stringify({
      compilerOptions: {
        lib: ["ES2022", "DOM"],
        module: "NodeNext",
        moduleResolution: "NodeNext",
        noEmit: true,
        skipLibCheck: true,
        strict: true,
        types: [],
      },
      include: ["stream.ts"],
    }),
  );
}

function smokeConsumer({ artifactRoot, mastraVersion, npmCache }) {
  const consumerRoot = join(
    smokeRoot,
    `consumer-mastra-${mastraVersion.replaceAll(".", "-")}`,
  );
  mkdirSync(consumerRoot);
  writeConsumerFiles(consumerRoot);
  const tarballs = metadata.packages.map(({ tarball }) =>
    join(artifactRoot, tarball),
  );
  run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--cache",
      npmCache,
      ...tarballs,
      `@mastra/core@${mastraVersion}`,
      "ai@7.0.65",
      "zod@3.25.76",
    ],
    consumerRoot,
  );
  run(process.execPath, ["index.mjs"], consumerRoot);
  run(
    join(repositoryRoot, "node_modules", ".bin", "tsc"),
    ["-p", "tsconfig.json"],
    consumerRoot,
  );
  run(process.execPath, ["generate.mjs"], consumerRoot);
  if (mastraVersion === upperMastraVersion) {
    run(
      join(repositoryRoot, "node_modules", ".bin", "tsc"),
      ["-p", "tsconfig.stream.json"],
      consumerRoot,
    );
  }
  run(process.execPath, ["stream.mjs", mastraVersion], consumerRoot);
}

const outputDirectory = parseOutputDirectory(process.argv.slice(2));
const metadata = await loadTypescriptPackageMetadata();
const smokeRoot = mkdtempSync(join(tmpdir(), "kitaru-package-smoke-"));
const artifactRoot = outputDirectory ?? join(smokeRoot, "artifacts");

try {
  mkdirSync(artifactRoot, { recursive: true });
  for (const packageEntry of metadata.packages) {
    run("pnpm", [
      "--filter",
      packageEntry.name,
      "pack",
      "--pack-destination",
      artifactRoot,
    ]);
    assertPackageContents(join(artifactRoot, packageEntry.tarball));
  }

  const npmCache = join(smokeRoot, "npm-cache");
  smokeConsumer({
    artifactRoot,
    mastraVersion: lowerMastraVersion,
    npmCache,
  });
  smokeConsumer({
    artifactRoot,
    mastraVersion: upperMastraVersion,
    npmCache,
  });
} finally {
  rmSync(smokeRoot, { force: true, recursive: true });
}
