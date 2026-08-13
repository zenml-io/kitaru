import { type ReplaySpec, ToolPolicyError } from "@zenml-io/kitaru";
import {
  assertInterceptableTool,
  assertSupportedToolPolicy,
} from "@zenml-io/kitaru/adapter";

import type { RuntimeGenerateOptions } from "./types.js";

// Mastra persists and recalls conversation history only when a run targets a
// thread, so dropping these options keeps a replay off live memory threads.
const LIVE_MEMORY_OPTIONS = [
  "memory",
  "resourceId",
  "savePerStep",
  "threadId",
] as const;

interface ToolListingAgent {
  listTools: (...args: never[]) => unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasToolListing(agent: unknown): agent is ToolListingAgent {
  return (
    typeof agent === "object" &&
    agent !== null &&
    "listTools" in agent &&
    typeof agent.listTools === "function"
  );
}

function toolEntries(tools: unknown): [string, unknown][] {
  return isRecord(tools) ? Object.entries(tools) : [];
}

/**
 * Remove the options that would read from or write to a live Mastra memory thread.
 */
export function stripLiveMemoryOptions(options: RuntimeGenerateOptions): void {
  for (const name of LIVE_MEMORY_OPTIONS) {
    delete options[name];
  }
}

/**
 * Reject a replay whose tools or approval settings escape the Kitaru tool hooks.
 */
export async function assertReplayToolCoverage(options: {
  agent: unknown;
  runtimeOptions: RuntimeGenerateOptions;
  spec: ReplaySpec;
}): Promise<void> {
  const { agent, runtimeOptions, spec } = options;
  if (
    runtimeOptions.requireToolApproval !== undefined &&
    runtimeOptions.requireToolApproval !== false
  ) {
    throw new ToolPolicyError("Replay does not support requireToolApproval");
  }
  const entries = [
    ...toolEntries(hasToolListing(agent) ? await agent.listTools() : undefined),
    ...toolEntries(runtimeOptions.clientTools),
    ...toolEntries(runtimeOptions.toolsets).flatMap(([, toolset]) =>
      toolEntries(toolset),
    ),
  ];
  for (const [toolName, tool] of entries) {
    assertInterceptableTool(
      toolName,
      isRecord(tool) && typeof tool.execute === "function",
    );
    assertSupportedToolPolicy(spec, toolName);
  }
}
