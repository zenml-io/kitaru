import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));

describe("core import boundary", () => {
  it("does not depend on agent frameworks or model providers", async () => {
    const sourceRoot = join(packageRoot, "src");
    const sourceFiles = (
      await readdir(sourceRoot, {
        recursive: true,
      })
    ).filter((path) => path.endsWith(".ts"));
    const sources = await Promise.all(
      sourceFiles.map((path) => readFile(join(sourceRoot, path), "utf8")),
    );
    const manifest = JSON.parse(
      await readFile(join(packageRoot, "package.json"), "utf8"),
    ) as Record<string, Record<string, string> | undefined>;
    const dependencies = {
      ...manifest.dependencies,
      ...manifest.optionalDependencies,
      ...manifest.peerDependencies,
    };

    expect(sources.join("\n")).not.toMatch(
      /(?:from|import)\s*\(?["'](?:@mastra\/|@ai-sdk\/|ai(?:\/|["']))/,
    );
    expect(Object.keys(dependencies)).not.toContain("ai");
    expect(Object.keys(dependencies)).not.toEqual(
      expect.arrayContaining([
        expect.stringMatching(/^@mastra\//),
        expect.stringMatching(/^@ai-sdk\//),
      ]),
    );
  });
});
