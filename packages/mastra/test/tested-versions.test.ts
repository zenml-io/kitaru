import { existsSync, readdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { assertMemoryReplayVersions } from "../src/memory-replay.js";
import {
  MEMORY_REPLAY_TESTED_VERSIONS,
  type MemoryReplayTestedVersions,
} from "../src/memory-replay-versions.js";
import { isSupportedMastraStreamVersion } from "../src/stream-recording.js";

interface PackageJson {
  devDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
}

const PACKAGES_DIR = new URL("../../", import.meta.url);
const COMPAT_DIR = new URL("mastra-compat/", PACKAGES_DIR);
// Every `packages/mastra-compat` package runs this file too, against the
// Mastra it installs; `packages/mastra` runs it without the variable.
const RUNNING_PACKAGE = process.env.KITARU_MASTRA_TEST_PACKAGE
  ? pathToFileURL(process.env.KITARU_MASTRA_TEST_PACKAGE)
  : new URL("../", import.meta.url);

function readPackageJson(packageDir: URL): PackageJson {
  return JSON.parse(readFileSync(new URL("package.json", packageDir), "utf8"));
}

function readPins(packageDir: URL): MemoryReplayTestedVersions {
  const pins = readPackageJson(packageDir).devDependencies ?? {};
  return {
    core: String(pins["@mastra/core"]),
    memory: String(pins["@mastra/memory"]),
    pg: String(pins["@mastra/pg"]),
  };
}

function listTestPackages(): URL[] {
  const compat = readdirSync(COMPAT_DIR, { withFileTypes: true })
    .map((entry) => new URL(`${entry.name}/`, COMPAT_DIR))
    .filter((dir) => existsSync(new URL("package.json", dir)));
  return [new URL("mastra/", PACKAGES_DIR), ...compat];
}

function byCore(
  left: MemoryReplayTestedVersions,
  right: MemoryReplayTestedVersions,
): number {
  return left.core.localeCompare(right.core, undefined, { numeric: true });
}

describe("the Mastra versions under test", () => {
  it("are the release set this test package pins", async () => {
    const pinned = readPins(RUNNING_PACKAGE);
    // The adapter's version checks read manifests through Node's resolver.
    const adapterRequire = createRequire(
      new URL("../src/memory-replay.ts", import.meta.url),
    );
    expect({
      core: adapterRequire("@mastra/core/package.json").version,
      memory: adapterRequire("@mastra/memory/package.json").version,
      pg: adapterRequire("@mastra/pg/package.json").version,
    }).toEqual(pinned);
    // Module imports go through Vitest's resolver instead.
    const manifest = "@mastra/memory/package.json";
    const { default: memory } = await import(manifest, {
      with: { type: "json" },
    });
    expect(memory.version).toBe(pinned.memory);

    expect(MEMORY_REPLAY_TESTED_VERSIONS).toContainEqual(pinned);
    expect(() => assertMemoryReplayVersions()).not.toThrow();
    expect(isSupportedMastraStreamVersion(pinned.core)).toBe(true);
  });

  it("have exactly one test package per tested release set", () => {
    expect(listTestPackages().map(readPins).sort(byCore)).toEqual(
      [...MEMORY_REPLAY_TESTED_VERSIONS].sort(byCore),
    );
  });

  it("match the adapter's @mastra/memory peer range", () => {
    const peer = readPackageJson(new URL("mastra/", PACKAGES_DIR))
      .peerDependencies?.["@mastra/memory"];
    expect(peer?.split(" || ")).toEqual([
      ...new Set(MEMORY_REPLAY_TESTED_VERSIONS.map(({ memory }) => memory)),
    ]);
  });
});

describe("assertMemoryReplayVersions", () => {
  it("accepts every tested pair", () => {
    for (const { core, memory } of MEMORY_REPLAY_TESTED_VERSIONS)
      expect(() => assertMemoryReplayVersions(core, memory)).not.toThrow();
  });

  it.each([
    ["an untested core", "1.72.0", "1.32.1"],
    ["a tested core with another core's memory", "1.68.0", "1.30.0"],
  ])("rejects %s, naming the tested pairs", (_case, core, memory) => {
    expect(() => assertMemoryReplayVersions(core, memory)).toThrow(
      expect.objectContaining({
        reason: "version_mismatch",
        message: expect.stringContaining(
          "@mastra/core@1.67.0 with @mastra/memory@1.30.0, @mastra/core@1.68.0 with @mastra/memory@1.31.0",
        ),
      }),
    );
  });
});
