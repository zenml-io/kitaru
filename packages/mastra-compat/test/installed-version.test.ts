import { createRequire } from "node:module";
import { expect, it } from "vitest";
import { isSupportedMastraStreamVersion } from "../../mastra/src/stream-recording.js";
import packageMetadata from "../package.json" with { type: "json" };

it("gives the adapter's stream gate this package's supported @mastra/core", () => {
  const pinned = packageMetadata.devDependencies["@mastra/core"];
  const adapterRequire = createRequire(
    new URL("../../mastra/src/stream-recording.ts", import.meta.url),
  );
  const installed = adapterRequire("@mastra/core/package.json").version;

  expect(installed).toBe(pinned);
  expect(isSupportedMastraStreamVersion(installed)).toBe(true);
});
