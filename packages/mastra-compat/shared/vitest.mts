import { readFileSync } from "node:fs";
import { relative } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vitest/config";
import { MEMORY_REPLAY_TESTED_VERSIONS } from "../../mastra/src/memory-replay-versions.js";

const PACKAGES_DIR = fileURLToPath(new URL("../..", import.meta.url));

// Heavy payload-budget, large-blob, and wall-clock-timeout suites that verify
// Kitaru's own bounds rather than intermediate Mastra point releases. Both
// endpoints of the supported range (`packages/mastra` at the oldest release
// and the newest compat package) always run the full suite; intermediate
// compat packages skip these unless KITARU_MASTRA_COMPAT_FULL=1 is set.
const INTERMEDIATE_COMPAT_EXCLUDES = [
  "mastra/test/file-blob-replay.test.ts",
  "mastra/test/memory-evidence-budget.test.ts",
  "mastra/test/memory-lease-lifecycle.test.ts",
  "mastra/test/memory-slow-kitaru.test.ts",
  "mastra/test/om-replay-tolerance.test.ts",
  "mastra/test/signed-url-history.test.ts",
];

const NEWEST_TESTED_CORE = [...MEMORY_REPLAY_TESTED_VERSIONS]
  .sort((left, right) =>
    left.core.localeCompare(right.core, undefined, { numeric: true }),
  )
  .at(-1)?.core;

function isNewestTestedRelease(packageDirUrl: URL): boolean {
  const manifest = JSON.parse(
    readFileSync(new URL("package.json", packageDirUrl), "utf8"),
  ) as { devDependencies?: Record<string, string> };
  return manifest.devDependencies?.["@mastra/core"] === NEWEST_TESTED_CORE;
}

// Node resolves a bare import from the importing file's directory, so the
// adapter tests and sources would load `packages/mastra`'s own Mastra install.
// Resolve every @mastra/* import from the version package instead, so the
// whole module graph shares that package's single copy of each release.
function resolveMastraFrom(anchor: string): Plugin {
  return {
    name: "kitaru-mastra-compat:resolve-mastra",
    enforce: "pre",
    resolveId(source, _importer, options) {
      if (!source.startsWith("@mastra/")) return null;
      return this.resolve(source, anchor, { ...options, skipSelf: true });
    },
  };
}

/** Run `packages/mastra` tests against the Mastra release this package installs. */
export function defineMastraCompatConfig(configUrl: string) {
  const anchor = fileURLToPath(configUrl);
  const packageDirUrl = new URL(".", configUrl);
  const packageDir = fileURLToPath(packageDirUrl);
  const packageTests = `${relative(PACKAGES_DIR, packageDir)}/test/**/*.test.ts`;
  const runFullSuite =
    process.env.KITARU_MASTRA_COMPAT_FULL === "1" ||
    isNewestTestedRelease(packageDirUrl);
  return defineConfig({
    plugins: [resolveMastraFrom(anchor)],
    test: {
      dir: PACKAGES_DIR,
      include: [packageTests, "mastra/test/*.test.ts"],
      exclude: runFullSuite ? [] : INTERMEDIATE_COMPAT_EXCLUDES,
      env: { KITARU_MASTRA_TEST_PACKAGE: packageDir },
      setupFiles: [fileURLToPath(new URL("./setup.mts", import.meta.url))],
    },
  });
}
