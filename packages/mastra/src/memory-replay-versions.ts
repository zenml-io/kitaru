/** A Mastra release set the memory replay factory's test suite passes on. */
export interface MemoryReplayTestedVersions {
  readonly core: string;
  readonly memory: string;
  /** The `@mastra/pg` release the PostgreSQL replay tests use with `core`. */
  readonly pg: string;
}

// Exact `@mastra/core` + `@mastra/memory` pairs whose full adapter suite
// passes; each row is one test package's pins (see test/tested-versions.test.ts).
// 1.69.0 shipped no memory or pg release, so it uses the ones built for 1.68.0.
export const MEMORY_REPLAY_TESTED_VERSIONS: readonly MemoryReplayTestedVersions[] =
  [
    { core: "1.67.0", memory: "1.30.0", pg: "1.25.0" },
    { core: "1.68.0", memory: "1.31.0", pg: "1.26.0" },
    { core: "1.69.0", memory: "1.31.0", pg: "1.26.0" },
    { core: "1.70.0", memory: "1.32.0", pg: "1.27.0" },
    { core: "1.71.0", memory: "1.32.1", pg: "1.27.1" },
    { core: "1.72.0", memory: "1.33.0", pg: "1.28.0" },
    { core: "1.73.0", memory: "1.34.0", pg: "1.28.1" },
    { core: "1.74.0", memory: "1.35.0", pg: "1.29.0" },
  ];
