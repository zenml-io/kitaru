import type { ToolHooks } from "@mastra/core/tools";
import { vi } from "vitest";

import type { RecordedStep } from "../src/step-recorder.js";
import type { RuntimeGenerateOptions } from "../src/types.js";

export const AGENT_ID = "018f0000-0000-7000-8000-000000000100";
export const REPLAY_ID = "018f0000-0000-7000-8000-000000000101";
export const ORIGINAL_SESSION_ID = "018f0000-0000-7000-8000-000000000102";
const REPLAY_JOB_ID = "018f0000-0000-7000-8000-000000000103";

export interface ApiCall {
  body: Record<string, unknown> | undefined;
  method: string;
  path: string;
}

export interface TestApiOptions {
  lookup?: (body: Record<string, unknown>) => unknown;
  replaySpec?: Record<string, unknown>;
}

export interface TestApi {
  calls: ApiCall[];
  nodeBatches(sessionId?: string): Record<string, unknown>[][];
  sessionIds: string[];
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": "application/json" },
    status,
  });
}

function sessionId(index: number): string {
  return `018f0000-0000-7000-8000-${String(index + 200).padStart(12, "0")}`;
}

function nodeId(index: number): string {
  return `018f0000-0000-7000-8001-${String(index + 300).padStart(12, "0")}`;
}

export function installTestApi(options: TestApiOptions = {}): TestApi {
  const calls: ApiCall[] = [];
  const sessionIds: string[] = [];
  const fetch = vi.fn<typeof globalThis.fetch>(async (input, init) => {
    const url = new URL(String(input));
    const method = init?.method ?? "GET";
    const body = init?.body
      ? (JSON.parse(String(init.body)) as Record<string, unknown>)
      : undefined;
    calls.push({ body, method, path: url.pathname });

    if (method === "POST" && url.pathname === "/api/v1/sessions") {
      const id = sessionId(sessionIds.length);
      sessionIds.push(id);
      return jsonResponse(
        { id, origin: "recorded", status: "in_progress" },
        201,
      );
    }
    if (method === "PATCH" && url.pathname.startsWith("/api/v1/sessions/")) {
      return jsonResponse({
        id: url.pathname.split("/").at(-1),
        origin: "recorded",
        status: body?.status ?? "in_progress",
      });
    }
    if (method === "POST" && url.pathname.endsWith("/nodes")) {
      const nodes = Array.isArray(body?.nodes) ? body.nodes : [];
      return jsonResponse(
        nodes.map((node) => ({
          id: nodeId(Number((node as Record<string, unknown>).index)),
          index: (node as Record<string, unknown>).index,
          node_type: (node as Record<string, unknown>).node_type,
          status: (node as Record<string, unknown>).status,
        })),
      );
    }
    if (method === "GET" && url.pathname === `/api/v1/replays/${REPLAY_ID}`) {
      return jsonResponse({
        job_id: REPLAY_JOB_ID,
        ...(options.replaySpec ?? {
          baseline_session_id: ORIGINAL_SESSION_ID,
          id: REPLAY_ID,
          override: null,
          status: "pending",
          tool_policy: { default: { type: "passthrough" }, tools: {} },
        }),
      });
    }
    if (method === "POST" && url.pathname.endsWith("/tool-lookup")) {
      return jsonResponse(
        options.lookup?.(body ?? {}) ?? { found: false, result: null },
      );
    }
    throw new Error(`Unexpected request: ${method} ${url.pathname}`);
  });
  vi.stubGlobal("fetch", fetch);

  return {
    calls,
    nodeBatches(selectedSessionId) {
      return calls
        .filter(
          (call) =>
            call.method === "POST" &&
            call.path.endsWith("/nodes") &&
            (selectedSessionId === undefined ||
              call.path.includes(selectedSessionId)),
        )
        .map((call) => call.body?.nodes as Record<string, unknown>[]);
    },
    sessionIds,
  };
}

export function textStep(suffix = "one"): RecordedStep {
  return {
    content: [{ text: `text-${suffix}`, type: "text" }],
    finishReason: "stop",
    model: {
      modelId: `requested-runtime-${suffix}`,
      provider: "test-provider",
      version: "v2",
    },
    providerMetadata: { test: { suffix } },
    request: { body: { request: suffix } },
    response: {
      id: `response-${suffix}`,
      messages: [{ content: `response-${suffix}`, role: "assistant" }],
      modelId: `effective-${suffix}`,
      timestamp: new Date(0),
    },
    toolCalls: [],
    toolResults: [],
    usage: {
      inputTokens: 3,
      outputTokens: 2,
      totalTokens: 5,
    },
    warnings: [],
  } as unknown as RecordedStep;
}

export function toolStep(
  callId: string,
  toolName: string,
  args: unknown,
  result: unknown,
): RecordedStep {
  return {
    ...textStep("tool"),
    content: [],
    finishReason: "tool-calls",
    toolCalls: [{ payload: { args, toolCallId: callId, toolName } }],
    toolResults: [{ payload: { args, result, toolCallId: callId, toolName } }],
  } as unknown as RecordedStep;
}

export class FakeAgent {
  readonly calls: Array<{
    messages: unknown;
    options: RuntimeGenerateOptions;
  }> = [];
  readonly #run: (
    messages: unknown,
    options: RuntimeGenerateOptions,
  ) => Promise<unknown>;

  constructor(
    run?: (
      messages: unknown,
      options: RuntimeGenerateOptions,
    ) => Promise<unknown>,
  ) {
    this.#run =
      run ??
      (async (_messages, runOptions) => {
        await runOptions.onStepFinish?.(textStep());
        return { text: "done" };
      });
  }

  async generate(
    messages: unknown,
    options: RuntimeGenerateOptions = {},
  ): Promise<unknown> {
    this.calls.push({ messages, options });
    return this.#run(messages, options);
  }

  async getToolsForExecution(
    options: RuntimeGenerateOptions = {},
  ): Promise<Record<string, unknown>> {
    return Object.assign(
      {},
      options.clientTools,
      ...Object.values(
        (options.toolsets as
          | Record<string, Record<string, unknown>>
          | undefined) ?? {},
      ),
    );
  }

  async getDefaultOptions(): Promise<Record<string, unknown>> {
    return {};
  }

  async listConfiguredInputProcessors(): Promise<unknown[]> {
    return [];
  }

  async listTools(): Promise<Record<string, unknown>> {
    return {};
  }
}

export async function invokeTool(
  hooks: ToolHooks,
  {
    args,
    callId,
    execute,
    output,
    toolName,
  }: {
    args: unknown;
    callId: string;
    execute?: () => Promise<unknown> | unknown;
    output: unknown;
    toolName: string;
  },
): Promise<unknown> {
  const context = { toolCallId: callId };
  const decision = await hooks.beforeToolCall?.({
    context,
    input: args,
    toolName,
  });
  if (decision?.proceed === false) {
    return decision.output;
  }
  const actualOutput = execute ? await execute() : output;
  await hooks.afterToolCall?.({
    context,
    input: args,
    output: actualOutput,
    toolName,
  });
  return actualOutput;
}
