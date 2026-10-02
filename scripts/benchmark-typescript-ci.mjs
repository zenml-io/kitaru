import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const [mode, outputPath, ...extraArguments] = process.argv.slice(2);
if (
  !["repeated", "coverage-once"].includes(mode) ||
  !outputPath ||
  extraArguments.length
) {
  console.error("Usage: benchmark-typescript-ci.mjs repeated|coverage-once OUTPUT_DIR");
  process.exit(1);
}
if (!process.env.KITARU_TEST_MASTRA_POSTGRES_URL) {
  console.error("KITARU_TEST_MASTRA_POSTGRES_URL is required for the benchmark");
  process.exit(1);
}

const repositoryRoot = fileURLToPath(new URL("..", import.meta.url));
const outputDirectory = resolve(outputPath);
const packages = ["kitaru", "kitaru-mastra", "kitaru-vercel-ai"];
const examples = [
  "kitaru-example-mastra-support-triage",
  "kitaru-example-mastra-adaptive-conversation",
];
const canonicalPackages = [
  "kitaru",
  "kitaru-mastra",
  "kitaru-mastra-compat-1.68",
  "kitaru-mastra-compat-1.69",
  "kitaru-mastra-compat-1.70",
  "kitaru-mastra-compat-1.71",
  "kitaru-vercel-ai",
  ...examples,
];
const runs = [];
mkdirSync(outputDirectory, { recursive: true });

function runSuite(packageName, { coverage, canonical, postgres }) {
  const outcomeDirectory = resolve(outputDirectory,
    canonical ? "outcomes" : "repeated-coverage-outcomes");
  mkdirSync(outcomeDirectory, { recursive: true });
  const args = [
    "--filter", `@zenml-io/${packageName}`, "exec", "vitest", "run",
    "--reporter=default", "--reporter=json",
    `--outputFile=${resolve(outcomeDirectory, `${packageName}.json`)}`,
  ];
  if (examples.includes(packageName)) {
    args.push("test");
  }
  if (coverage) {
    args.push(
      "--coverage.enabled", "--coverage.provider=v8",
      "--coverage.include=src/**", "--coverage.exclude=src/generated/**",
      "--coverage.reporter=text-summary", "--coverage.reporter=json",
      `--coverage.reportsDirectory=${resolve(outputDirectory, "coverage", packageName)}`,
    );
  }
  const env = { ...process.env };
  if (!postgres) {
    delete env.KITARU_TEST_MASTRA_POSTGRES_URL;
    delete env.POSTGRES_PASSWORD;
  }
  const started = performance.now();
  const result = spawnSync("pnpm", args, {
    cwd: repositoryRoot,
    env,
    stdio: ["inherit", "pipe", "inherit"],
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  process.stdout.write(result.stdout ?? "");
  runs.push({
    package: packageName,
    canonical,
    coverage,
    postgres,
    command: ["pnpm", ...args],
    elapsedMs: performance.now() - started,
    exitCode: result.status,
  });
  writeFileSync(
    resolve(outputDirectory, "runs.json"),
    `${JSON.stringify({ mode, sha: process.env.GITHUB_SHA ?? null, runs }, null, 2)}\n`,
  );
  if (result.error || result.status !== 0) {
    console.error(result.error ?? `${packageName} failed with exit code ${result.status}`);
    process.exit(result.status ?? 1);
  }
  if (coverage) {
    const summaryLines = (result.stdout ?? "").split("\n")
      .filter((line) => /^(Statements|Branches|Functions|Lines) /.test(line));
    if (!["Statements", "Branches", "Functions", "Lines"].every(
      (label) => summaryLines.some((line) => line.startsWith(`${label} `)),
    )) {
      console.error(`${packageName} omitted an expected coverage summary metric`);
      process.exit(1);
    }
    if (process.env.GITHUB_STEP_SUMMARY) {
      appendFileSync(
        process.env.GITHUB_STEP_SUMMARY,
        `### \`@zenml-io/${packageName}\` coverage\n\n\`\`\`text\n${summaryLines.join("\n")}\n\`\`\`\n`,
      );
    }
  }
}

for (const packageName of canonicalPackages) {
  runSuite(packageName, {
    coverage: mode === "coverage-once" && packages.includes(packageName),
    canonical: true, postgres: true,
  });
}
if (mode === "repeated") {
  for (const packageName of packages) {
    runSuite(packageName, { coverage: true, canonical: false, postgres: false });
  }
}
