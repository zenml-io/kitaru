import { relative } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vitest/config";

const PACKAGES_DIR = fileURLToPath(new URL("../..", import.meta.url));

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

/** Run every `packages/mastra` test against the Mastra this package installs. */
export function defineMastraCompatConfig(configUrl: string) {
  const anchor = fileURLToPath(configUrl);
  const packageDir = fileURLToPath(new URL(".", configUrl));
  const packageTests = `${relative(PACKAGES_DIR, packageDir)}/test/**/*.test.ts`;
  return defineConfig({
    plugins: [resolveMastraFrom(anchor)],
    test: {
      dir: PACKAGES_DIR,
      include: [packageTests, "mastra/test/*.test.ts"],
      env: { KITARU_MASTRA_TEST_PACKAGE: packageDir },
      setupFiles: [fileURLToPath(new URL("./setup.mts", import.meta.url))],
    },
  });
}
