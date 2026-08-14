import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import * as rootExports from "../src/index.js";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));

describe("core import boundary", () => {
  it("keeps Node filesystem access out of the root import graph", async () => {
    const rootSource = await readFile(
      join(packageRoot, "src", "index.ts"),
      "utf8",
    );
    const runtimeNeutralSources = await Promise.all(
      ["auth/index.ts", "client.ts", "environment.ts", "transport.ts"].map(
        (path) => readFile(join(packageRoot, "src", path), "utf8"),
      ),
    );

    expect(rootSource).not.toMatch(/\.\/node(?:\/|\.js)/);
    expect(runtimeNeutralSources.join("\n")).not.toMatch(
      /(?:from|import)\s*\(?["']node:(?:fs|os|path)/,
    );
  });

  it("does not publish internal resource constructors from the package root", () => {
    expect(Object.keys(rootExports)).not.toEqual(
      expect.arrayContaining([
        "AccountsResource",
        "AgentsResource",
        "JobsResource",
        "ReplaysResource",
        "SessionsResource",
        "TasksResource",
      ]),
    );
  });

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
