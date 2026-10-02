import { fileURLToPath } from "node:url";
import { configDefaults, defineConfig, type Plugin } from "vitest/config";

// The memory replay factory requires exactly the @mastra/core version
// `packages/mastra` installs, and `@mastra/memory` loads that copy directly,
// so its tests stay there. Every other adapter test runs here too; a new
// memory factory test fails here until it is listed.
const MEMORY_FACTORY_TESTS = [
  "attachment-tokens",
  "file-blob-replay",
  "implicit-om",
  "memory-binding",
  "memory-evidence-budget",
  "memory-lease-lifecycle",
  "memory-lease-process",
  "memory-replay",
  "memory-replay-safety",
  "memory-slow-kitaru",
  "memory-snapshot",
  "native-memory-replay",
  "om-production-row",
  "om-replay-tolerance",
  "om-result-tape",
  "postgres-memory-replay",
  "processor-replay",
  "processor-decision-replay",
  "replay-clock",
  "replay-diagnostics",
  "replay-fidelity",
  "request-capture",
  "secret-key-policy",
  "signed-url-history",
  "stateful-files",
  "stateful-overrides",
  "stateful-tools",
  "stateful-workspace",
];

// Node resolves a bare import from the importing file's directory, so the
// adapter tests and sources would load `packages/mastra`'s own @mastra/core.
// Resolve every @mastra/core import from this package instead, so the whole
// module graph shares this package's single copy.
function resolveMastraCoreFromThisPackage(): Plugin {
  const anchor = fileURLToPath(import.meta.url);
  return {
    name: "kitaru-mastra-compat:resolve-mastra-core",
    enforce: "pre",
    resolveId(source, _importer, options) {
      if (source !== "@mastra/core" && !source.startsWith("@mastra/core/")) {
        return null;
      }
      return this.resolve(source, anchor, { ...options, skipSelf: true });
    },
  };
}

export default defineConfig({
  plugins: [resolveMastraCoreFromThisPackage()],
  test: {
    dir: fileURLToPath(new URL("..", import.meta.url)),
    include: ["mastra-compat/test/**/*.test.ts", "mastra/test/*.test.ts"],
    exclude: [
      ...configDefaults.exclude,
      ...MEMORY_FACTORY_TESTS.map((name) => `mastra/test/${name}.test.ts`),
    ],
    setupFiles: ["./test/setup.ts"],
  },
});
