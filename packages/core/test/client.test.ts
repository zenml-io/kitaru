import { afterEach, describe, expect, it, vi } from "vitest";

import {
  KitaruClient,
  type SessionCreateRequest,
  type SessionNodeBatchRequest,
  type ToolLookupRequest,
} from "../src/client.js";
import { KitaruApiError } from "../src/errors.js";

const SESSION_ID = "018f0000-0000-7000-8000-000000000001";
const NODE_ID = "018f0000-0000-7000-8000-000000000002";
const REPLAY_ID = "018f0000-0000-7000-8000-000000000003";
const ORIGINAL_SESSION_ID = "018f0000-0000-7000-8000-000000000004";

const sessionResponse = {
  id: SESSION_ID,
  origin: "recorded",
  status: "in_progress",
};

const createRequest = {
  agent_id: "agent-id",
  expected: null,
  inputs: null,
  origin: "recorded",
  outputs: null,
} as SessionCreateRequest;

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("KitaruClient", () => {
  it("prefers explicit configuration, normalizes the URL, and authenticates", async () => {
    vi.stubEnv("KITARU_API_URL", "https://environment.example");
    vi.stubEnv("KITARU_API_KEY", "environment-secret");
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(sessionResponse, 201),
    );
    const client = new KitaruClient({
      apiUrl: "https://explicit.example/",
      apiKey: "explicit-secret",
      fetch,
    });

    await client.createSession(createRequest);

    expect(fetch).toHaveBeenCalledOnce();
    const [url, init] = fetch.mock.calls[0] ?? [];
    expect(url).toBe("https://explicit.example/v1/sessions");
    expect(init?.headers).toMatchObject({
      Authorization: "Bearer explicit-secret",
    });
  });

  it("surfaces FastAPI detail without exposing validation input", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        {
          detail: [
            {
              input: "private prompt",
              loc: ["body", "inputs"],
              msg: "Input should be valid",
            },
          ],
        },
        422,
      ),
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });

    const error = await client
      .createSession(createRequest)
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruApiError);
    expect(error).toMatchObject({
      method: "POST",
      path: "/v1/sessions",
      status: 422,
    });
    expect((error as Error).message).toContain(
      "body.inputs: Input should be valid",
    );
    expect((error as Error).message).not.toContain("private prompt");
  });

  it("does not expose a raw string error detail", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ detail: "private prompt and credential" }, 400),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      message: expect.not.stringContaining("private prompt"),
      status: 400,
    });
  });

  it("aborts requests after the configured timeout", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(
      (_input, init): Promise<Response> =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new DOMException("Aborted", "AbortError"));
          });
        }),
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 5,
    });

    const error = await client
      .createSession(createRequest)
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruApiError);
    expect(error).toMatchObject({ status: null });
    expect((error as Error).message).toContain("timed out after 5ms");
  });

  it("keeps the timeout active while reading the response body", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(
      async (_input, init) =>
        ({
          json: () =>
            new Promise((_resolve, reject) => {
              init?.signal?.addEventListener("abort", () => {
                reject(new DOMException("Aborted", "AbortError"));
              });
            }),
          ok: true,
          status: 201,
          statusText: "Created",
        }) as Response,
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 5,
    });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      status: null,
      message: expect.stringContaining("timed out after 5ms"),
    });
  });

  it("rejects responses that omit required IDs or discriminators", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ origin: "recorded", status: "in_progress" }, 201),
      )
      .mockResolvedValueOnce(
        jsonResponse({ ...sessionResponse, origin: "invalid" }, 201),
      )
      .mockResolvedValueOnce(
        jsonResponse([
          { id: NODE_ID, node_type: "invalid", status: "completed" },
        ]),
      );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      method: "POST",
      path: "/v1/sessions",
      status: 201,
      message: expect.stringContaining("missing id"),
    });
    await expect(client.createSession(createRequest)).rejects.toThrow(
      "invalid origin",
    );
    await expect(
      client.upsertSessionNodes(SESSION_ID, { nodes: [] }),
    ).rejects.toThrow("invalid node_type");
  });

  it("retries node upsert with the identical body and node indexes", async () => {
    const storedNode = {
      id: NODE_ID,
      node_type: "llm_call",
      status: "completed",
    };
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ detail: "unavailable" }, 503))
      .mockResolvedValueOnce(jsonResponse([storedNode]));
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });
    const request = {
      nodes: [
        {
          attributes: {},
          index: 1,
          inputs: null,
          name: "model_request",
          node_type: "llm_call",
          outputs: null,
          status: "completed",
        },
      ],
    } as SessionNodeBatchRequest;

    await expect(
      client.upsertSessionNodes(SESSION_ID, request),
    ).resolves.toEqual([storedNode]);

    expect(fetch).toHaveBeenCalledTimes(2);
    const firstBody = fetch.mock.calls[0]?.[1]?.body;
    const secondBody = fetch.mock.calls[1]?.[1]?.body;
    expect(firstBody).toBe(secondBody);
    expect(JSON.parse(String(secondBody))).toEqual(request);
  });

  it("retries node upsert after a transport failure", async () => {
    const storedNode = {
      id: NODE_ID,
      node_type: "tool_call",
      status: "completed",
    };
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockRejectedValueOnce(new TypeError("network failed"))
      .mockResolvedValueOnce(jsonResponse([storedNode]));
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });
    const request = {
      nodes: [
        {
          attributes: {},
          index: 1,
          inputs: null,
          name: "lookup",
          node_type: "tool_call",
          outputs: null,
          status: "completed",
        },
      ],
    } as SessionNodeBatchRequest;

    await expect(
      client.upsertSessionNodes(SESSION_ID, request),
    ).resolves.toEqual([storedNode]);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("validates replay IDs and lookup discriminators", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({
          baseline_session_id: ORIGINAL_SESSION_ID,
          id: "not-a-uuid",
          status: "pending",
          tool_policy: { default: { type: "passthrough" } },
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ result: null }));
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });

    await expect(client.getReplay(REPLAY_ID)).rejects.toThrow("invalid id");
    await expect(
      client.lookupToolResult(REPLAY_ID, {
        cache_key: "a".repeat(64),
        tool_name: "normalize",
      } satisfies ToolLookupRequest),
    ).rejects.toThrow("missing found discriminator");
  });

  it.each([
    ["missing default", { tools: {} }],
    ["unknown default", { default: { type: "unknown" } }],
    [
      "malformed override",
      { default: { type: "passthrough" }, tools: { write: null } },
    ],
    ["incomplete history", { default: { type: "history" } }],
    ["incomplete static", { default: { type: "static", on_miss: "fail" } }],
  ])("rejects a replay with %s policy", async (_name, toolPolicy) => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({
        baseline_session_id: ORIGINAL_SESSION_ID,
        id: REPLAY_ID,
        status: "pending",
        tool_policy: toolPolicy,
      }),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.getReplay(REPLAY_ID)).rejects.toThrow(
      "Invalid response",
    );
  });

  it("does not retry session creation", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ detail: "unavailable" }, 503),
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
    });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      status: 503,
    });
    expect(fetch).toHaveBeenCalledOnce();
  });
});
