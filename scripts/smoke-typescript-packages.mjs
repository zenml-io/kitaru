import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const smokeRoot = mkdtempSync(join(tmpdir(), "kitaru-package-smoke-"));
const consumerRoot = join(smokeRoot, "consumer");

function run(command, args, cwd = repositoryRoot) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit" });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} exited with ${result.status}`);
  }
}

try {
  for (const packageName of [
    "@zenml-io/kitaru",
    "@zenml-io/kitaru-mastra",
    "@zenml-io/kitaru-vercel-ai",
  ]) {
    run("pnpm", ["--filter", packageName, "pack", "--pack-destination", smokeRoot]);
  }

  mkdirSync(consumerRoot);
  writeFileSync(
    join(consumerRoot, "package.json"),
    JSON.stringify({ name: "kitaru-package-smoke", private: true, type: "module" }),
  );
  writeFileSync(
    join(consumerRoot, "index.mjs"),
    `import { KitaruClient } from "@zenml-io/kitaru";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";
import { createKitaruGenerateText } from "@zenml-io/kitaru-vercel-ai";
if (![KitaruClient, KitaruAgent, createKitaruGenerateText].every(Boolean)) {
  throw new Error("Packed package exports are missing");
}
`,
  );
  writeFileSync(
    join(consumerRoot, "core.ts"),
    `import { KitaruClient, type KitaruEnvironmentVariables } from "@zenml-io/kitaru";
const environment: KitaruEnvironmentVariables = { KITARU_API_URL: "http://localhost" };
new KitaruClient({ apiUrl: environment.KITARU_API_URL });
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
        strict: true,
        types: [],
      },
      include: ["core.ts"],
    }),
  );

  const tarballs = [
    "zenml-io-kitaru-0.1.0-experimental.0.tgz",
    "zenml-io-kitaru-mastra-0.1.0-experimental.0.tgz",
    "zenml-io-kitaru-vercel-ai-0.1.0-experimental.0.tgz",
  ].map((name) => join(smokeRoot, name));
  run(
    "npm",
    [
      "install",
      "--ignore-scripts",
      "--cache",
      join(smokeRoot, "npm-cache"),
      ...tarballs,
      "@mastra/core@1.51.0",
      "ai@7.0.55",
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
