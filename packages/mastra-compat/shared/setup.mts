import { vi } from "vitest";

// The adapter reads Mastra manifests and modules through `createRequire`
// (the stream version gate, the memory factory's version check and schema
// helpers). Node's own resolver bypasses the Vitest config, so answer every
// @mastra/* lookup from the version package's install instead of
// `packages/mastra`'s, keeping those reads on the Mastra that actually runs.
vi.mock("node:module", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:module")>();
  const packageDir = process.env.KITARU_MASTRA_TEST_PACKAGE;
  if (!packageDir) throw new Error("KITARU_MASTRA_TEST_PACKAGE is not set.");
  const mastraRequire = actual.createRequire(`${packageDir}package.json`);
  return {
    ...actual,
    createRequire(path: string | URL) {
      const original = actual.createRequire(path);
      return (id: string) =>
        id.startsWith("@mastra/") ? mastraRequire(id) : original(id);
    },
  };
});
