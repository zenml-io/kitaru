import { vi } from "vitest";

const MASTRA_CORE_MANIFEST = "@mastra/core/package.json";

// The adapter reads the @mastra/core manifest through `createRequire` to gate
// `KitaruAgent.stream()`. Node's own resolver bypasses the Vitest config, so
// answer that lookup from this package's install instead of
// `packages/mastra`'s, keeping the gate on the version that actually runs.
vi.mock("node:module", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:module")>();
  const manifest: unknown = actual.createRequire(import.meta.url)(
    MASTRA_CORE_MANIFEST,
  );
  return {
    ...actual,
    createRequire(path: string | URL) {
      const original = actual.createRequire(path);
      return (id: string) =>
        id === MASTRA_CORE_MANIFEST ? manifest : original(id);
    },
  };
});
