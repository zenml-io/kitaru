import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from "@mastra/core/request-context";
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

interface ToolInventoryAgent {
  getDefaultOptions: (options?: { requestContext?: RequestContext }) => unknown;
  getToolsForExecution: (options: Record<string, unknown>) => unknown;
  listConfiguredInputProcessors: (requestContext?: RequestContext) => unknown;
  listTools: (options?: { requestContext?: RequestContext }) => unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasToolInventory(agent: unknown): agent is ToolInventoryAgent {
  return (
    typeof agent === "object" &&
    agent !== null &&
    "getDefaultOptions" in agent &&
    typeof agent.getDefaultOptions === "function" &&
    "getToolsForExecution" in agent &&
    typeof agent.getToolsForExecution === "function" &&
    "listConfiguredInputProcessors" in agent &&
    typeof agent.listConfiguredInputProcessors === "function" &&
    "listTools" in agent &&
    typeof agent.listTools === "function"
  );
}

function toolEntries(tools: unknown): [string, unknown][] {
  return isRecord(tools) ? Object.entries(tools) : [];
}

function formatMastraToolName(toolName: string): string {
  let formatted = toolName.replace(/[^a-zA-Z0-9_-]/g, "_");
  if (!/^[a-zA-Z_]/.test(formatted)) {
    formatted = `_${formatted}`;
  }
  return formatted.slice(0, 63);
}

function assertStableToolName(toolName: string): void {
  const runtimeName = formatMastraToolName(toolName);
  if (runtimeName !== toolName) {
    throw new ToolPolicyError(
      `Replay cannot safely apply a policy for tool '${toolName}' because Mastra exposes it to the model as '${runtimeName}'`,
    );
  }
}

/**
 * Remove the options that would read from or write to a live Mastra memory thread.
 */
export function stripLiveMemoryOptions(options: RuntimeGenerateOptions): void {
  for (const name of LIVE_MEMORY_OPTIONS) {
    delete options[name];
  }
  const requestContext = new RequestContext(options.requestContext?.entries());
  requestContext.delete(MASTRA_RESOURCE_ID_KEY);
  requestContext.delete(MASTRA_THREAD_ID_KEY);
  options.requestContext = requestContext;
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
  if (!hasToolInventory(agent)) {
    throw new ToolPolicyError(
      "Replay requires Mastra Agent tool inventory support",
    );
  }

  const defaultOptions = await agent.getDefaultOptions({
    requestContext: runtimeOptions.requestContext,
  });
  const configuredDefaults = isRecord(defaultOptions) ? defaultOptions : {};
  if (
    (runtimeOptions.requireToolApproval ??
      configuredDefaults.requireToolApproval) !== undefined &&
    (runtimeOptions.requireToolApproval ??
      configuredDefaults.requireToolApproval) !== false
  ) {
    throw new ToolPolicyError("Replay does not support requireToolApproval");
  }
  if (
    (runtimeOptions.experimental_sandbox ??
      configuredDefaults.experimental_sandbox) !== undefined
  ) {
    throw new ToolPolicyError("Replay does not support sandboxed tools");
  }
  for (const name of LIVE_MEMORY_OPTIONS) {
    const value = configuredDefaults[name];
    if (name === "savePerStep" ? value === true : value !== undefined) {
      throw new ToolPolicyError(
        `Replay does not support default Mastra option '${name}'`,
      );
    }
  }
  if (
    runtimeOptions.prepareStep !== undefined ||
    configuredDefaults.prepareStep !== undefined
  ) {
    throw new ToolPolicyError("Replay does not support prepareStep");
  }
  const inputProcessors =
    runtimeOptions.inputProcessors ??
    configuredDefaults.inputProcessors ??
    (await agent.listConfiguredInputProcessors(runtimeOptions.requestContext));
  if (!Array.isArray(inputProcessors) || inputProcessors.length > 0) {
    throw new ToolPolicyError("Replay does not support input processors");
  }

  const configuredEntries = [
    ...toolEntries(
      await agent.listTools({
        requestContext: runtimeOptions.requestContext,
      }),
    ),
    ...toolEntries(configuredDefaults.clientTools),
    ...toolEntries(configuredDefaults.toolsets).flatMap(([, toolset]) =>
      toolEntries(toolset),
    ),
    ...toolEntries(runtimeOptions.clientTools),
    ...toolEntries(runtimeOptions.toolsets).flatMap(([, toolset]) =>
      toolEntries(toolset),
    ),
  ];
  for (const [toolName] of configuredEntries) {
    assertStableToolName(toolName);
  }

  const executableTools = await agent.getToolsForExecution({
    autoResumeSuspendedTools: runtimeOptions.autoResumeSuspendedTools,
    clientTools: runtimeOptions.clientTools,
    delegation: runtimeOptions.delegation,
    memoryConfig: runtimeOptions.memory,
    methodType: "generate",
    outputWriter: runtimeOptions.outputWriter,
    requestContext: runtimeOptions.requestContext,
    resourceId: runtimeOptions.resourceId,
    runId: runtimeOptions.runId,
    threadId: runtimeOptions.threadId,
    toolsets: runtimeOptions.toolsets,
  });
  for (const [toolName, tool] of toolEntries(executableTools)) {
    assertInterceptableTool(
      toolName,
      isRecord(tool) && typeof tool.execute === "function",
    );
    assertSupportedToolPolicy(spec, toolName);
  }
}
