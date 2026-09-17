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
