import { afterEach, describe, expect, it, vi } from "vitest";

import {
  reportStreamRecordingError,
  runStreamingSupport,
  STREAM_TEXT,
} from "../src/stream.js";

const AGENT_ID = "018f0000-0000-7000-8000-000000000300";
const SESSION_ID = "018f0000-0000-7000-8000-000000000301";

interface ApiCall {
  body?: Record<string, unknown>;
  method: string;
  path: string;
}

function installApi(): ApiCall[] {
  const calls: ApiCall[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>(async (input, init = {}) => {
      const url = new URL(String(input));
      const method = init.method ?? "GET";
      const body = init.body
        ? (JSON.parse(String(init.body)) as Record<string, unknown>)
        : undefined;
      calls.push({ body, method, path: url.pathname });
      if (method === "POST" && url.pathname === "/api/v1/sessions") {
        return Response.json(
          { id: SESSION_ID, origin: "recorded", status: "in_progress" },
          { status: 201 },
        );
      }
      if (method === "POST" && url.pathname.endsWith("/nodes")) {
        const nodes = Array.isArray(body?.nodes) ? body.nodes : [];
        return Response.json(
          nodes.map((node, index) => ({
            external_id: (node as Record<string, unknown>).external_id,
            id: `018f0000-0000-7000-8001-${String(index).padStart(12, "0")}`,
            node_type: (node as Record<string, unknown>).node_type,
            status: (node as Record<string, unknown>).status,
          })),
        );
      }
      if (method === "PATCH" && url.pathname.endsWith(SESSION_ID)) {
        return Response.json({
          id: SESSION_ID,
          origin: "recorded",
          status: body?.status,
        });
      }
      throw new Error(`Unexpected request: ${method} ${url.pathname}`);
    }),
  );
  return calls;
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("streaming support example", () => {
  it("emits two native chunks and records the local tool result", async () => {
    const calls = installApi();
    const chunks: string[] = [];

    const result = await runStreamingSupport({
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      onChunk: (chunk) => chunks.push(chunk),
    });

    expect(result).toEqual({ aborted: false, chunks: 2, text: STREAM_TEXT });
    expect(chunks).toEqual(["Order ord-1001 ", "is delayed."]);
    const nodes = calls
      .filter((call) => call.path.endsWith("/nodes"))
      .flatMap((call) => call.body?.nodes ?? []) as Record<string, unknown>[];
    expect(nodes.filter((node) => node.node_type === "llm_call")).toHaveLength(
      2,
    );
    expect(nodes.find((node) => node.node_type === "tool_call")).toMatchObject({
      inputs: { orderId: "ord-1001" },
      status: "completed",
    });
    expect(calls.at(-1)?.body).toMatchObject({
      outputs: { step_count: 2, text: STREAM_TEXT },
      status: "completed",
    });
  });

  it("reports only the bounded recording location", () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});

    reportStreamRecordingError({
      error: new Error("prompt and token must stay private"),
      sessionId: SESSION_ID,
      stage: "complete",
    });

    expect(error).toHaveBeenCalledWith(
      `Kitaru recording failed at complete for session ${SESSION_ID}`,
    );
    expect(JSON.stringify(error.mock.calls)).not.toContain("prompt and token");
  });
});
