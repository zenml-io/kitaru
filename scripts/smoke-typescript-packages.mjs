import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { loadTypescriptPackageMetadata } from "./typescript-packages.mjs";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));

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

const outputDirectory = parseOutputDirectory(process.argv.slice(2));
const metadata = await loadTypescriptPackageMetadata();
const smokeRoot = mkdtempSync(join(tmpdir(), "kitaru-package-smoke-"));
const consumerRoot = join(smokeRoot, "consumer");
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

  mkdirSync(consumerRoot);
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
    `import { KitaruClient, type KitaruEnvironmentVariables } from "@zenml-io/kitaru";
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
new KitaruAgent({ generate: async () => ({}) }, mastraOptions);
const vercelOptions: KitaruVercelAIOptions = { agentId: "package-smoke" };
createKitaruGenerateText(vercelOptions);
declare const agentSettings: KitaruToolLoopAgentSettings;
createKitaruToolLoopAgent(agentSettings, vercelOptions);
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

  const tarballs = metadata.packages.map(({ tarball }) =>
    join(artifactRoot, tarball),
  );
  run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--cache",
      join(smokeRoot, "npm-cache"),
      ...tarballs,
      "@mastra/core@1.51.0",
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
} finally {
  rmSync(smokeRoot, { force: true, recursive: true });
}
