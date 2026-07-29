import fs from "node:fs";
import path from "node:path";

import type { PrivateHeldoutLabel, PublicTrace } from "./types.js";

const appDirectory = import.meta.filename.endsWith(".ts")
  ? path.resolve(import.meta.dirname, "..")
  : path.resolve(import.meta.dirname, "../..");
const fixtureDirectory = path.join(appDirectory, "fixtures");

function readFixture<T>(filename: string): T {
  return JSON.parse(
    fs.readFileSync(path.join(fixtureDirectory, filename), "utf8"),
  ) as T;
}

export const discoveryTraces = readFixture<PublicTrace[]>(
  "discovery-traces.json",
);
export const heldoutTraces = readFixture<PublicTrace[]>("heldout-traces.json");

// This module is the only place that loads gold labels. Public trace tools build
// their responses from discoveryTraces and heldoutTraces, never by redacting a
// joined trace-plus-label object.
export const privateHeldoutLabels = readFixture<PrivateHeldoutLabel[]>(
  "private-heldout-labels.json",
);

export const discoveryById = new Map(
  discoveryTraces.map((trace) => [trace.id, trace]),
);
export const heldoutById = new Map(
  heldoutTraces.map((trace) => [trace.id, trace]),
);
export const privateLabelsByFamily = new Map(
  (["permission-boundary", "insufficient-evidence"] as const).map((family) => [
    family,
    new Map(
      privateHeldoutLabels
        .filter((label) => label.family === family)
        .map((label) => [label.traceId, label]),
    ),
  ]),
);
