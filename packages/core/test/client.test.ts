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
const TASK_ID = "018f0000-0000-7000-8000-000000000005";
const RESULT_SESSION_ID = "018f0000-0000-7000-8000-000000000006";

const sessionResponse = {
  id: SESSION_ID,
  origin: "recorded",
  status: "in_progress",
};

function taskResponse(resultSessionId: string | null): unknown {
  return {
    attempt: 1,
    id: TASK_ID,
    job_id: "018f0000-0000-7000-8000-000000000007",
    kind: "agent",
    labels: {},
    on_failure: "abort",
    result: null,
    result_session_id: resultSessionId,
    status: "running",
  };
}

function conflictResponse(): Response {
  return jsonResponse(
    { detail: `Task ${TASK_ID} already links a result session` },
    409,
  );
}

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
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("KitaruClient", () => {
  it("accepts explicit configuration when process is unavailable", () => {
    vi.stubGlobal("process", undefined);

    expect(
      () =>
        new KitaruClient({
          apiKey: "explicit-secret",
          apiUrl: "https://api.example",
        }),
    ).not.toThrow();
  });

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
    expect(url).toBe("https://explicit.example/api/v1/sessions");
    expect(init?.headers).toMatchObject({
      Authorization: "Bearer explicit-secret",
      "X-Kitaru-Client": "kitaru-typescript",
    });
    expect(init?.headers).not.toHaveProperty("X-Kitaru-Skill");
  });

  it("sends the active skill named in the environment", async () => {
    vi.stubEnv("KITARU_ACTIVE_SKILL", "data-analysis");
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(sessionResponse, 201),
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      apiKey: "secret",
      fetch,
    });

    await client.createSession(createRequest);

    const [, init] = fetch.mock.calls[0] ?? [];
    expect(init?.headers).toMatchObject({
      "X-Kitaru-Skill": "data-analysis",
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
      path: "/api/v1/sessions",
      status: 422,
    });
    expect((error as Error).message).toContain(
      "body.inputs: Input should be valid",
    );
    expect((error as Error).message).not.toContain("private prompt");
  });

  it("reports a string error detail like the Python SDK", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(
        { detail: "Session names no agent and no task to infer one from" },
        422,
      ),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      message: expect.stringContaining(
        "Session names no agent and no task to infer one from",
      ),
      status: 422,
    });
  });

  it("falls back to the status text for a blank error detail", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ detail: "   " }, 404),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.createSession(createRequest)).rejects.toMatchObject({
      message: expect.stringContaining("POST /api/v1/sessions"),
      status: 404,
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
      path: "/api/v1/sessions",
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

  it("loads an agent task spec for inputs omitted from the environment", async () => {
    const taskSpec = {
      details: { inputs: "stored prompt", kind: "agent" },
      env: {},
      kind: "agent",
      secret_env: {},
      task_id: TASK_ID,
      timeout_seconds: 60,
    };
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(taskSpec),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.getTaskSpec(TASK_ID)).resolves.toEqual(taskSpec);
    expect(fetch).toHaveBeenCalledWith(
      `https://api.example/api/v1/tasks/${TASK_ID}/spec`,
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("creates a session and skips task recovery on success", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(sessionResponse, 201),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.createOrGetResultSession(createRequest, TASK_ID),
    ).resolves.toEqual(sessionResponse);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("recovers the task's result session on a 409 conflict", async () => {
    const recoveredSession = {
      id: RESULT_SESSION_ID,
      origin: "recorded",
      status: "in_progress",
    };
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(conflictResponse())
      .mockResolvedValueOnce(jsonResponse(taskResponse(RESULT_SESSION_ID)))
      .mockResolvedValueOnce(jsonResponse(recoveredSession));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.createOrGetResultSession(createRequest, TASK_ID),
    ).resolves.toEqual(recoveredSession);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(fetch.mock.calls[1]?.[0]).toBe(
      `https://api.example/api/v1/tasks/${TASK_ID}`,
    );
    expect(fetch.mock.calls[2]?.[0]).toBe(
      `https://api.example/api/v1/sessions/${RESULT_SESSION_ID}`,
    );
  });

  it("re-raises the 409 when the task has no result session", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(conflictResponse())
      .mockResolvedValueOnce(jsonResponse(taskResponse(null)));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.createOrGetResultSession(createRequest, TASK_ID),
    ).rejects.toMatchObject({ status: 409 });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("re-raises the 409 without a task id to recover from", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(conflictResponse());
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.createOrGetResultSession(createRequest),
    ).rejects.toMatchObject({ status: 409 });
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("re-raises a non-conflict error without attempting recovery", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ detail: "not found" }, 404));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.createOrGetResultSession(createRequest, TASK_ID),
    ).rejects.toMatchObject({ status: 404 });
    expect(fetch).toHaveBeenCalledOnce();
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
          job_id: SESSION_ID,
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
        job_id: SESSION_ID,
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

  it("keeps all six adapter requests route- and body-compatible", async () => {
    const nodeResponse = {
      id: NODE_ID,
      node_type: "span",
      status: "completed",
    };
    const replayResponse = {
      baseline_session_id: ORIGINAL_SESSION_ID,
      id: REPLAY_ID,
      job_id: SESSION_ID,
      status: "pending",
      tool_policy: { default: { type: "passthrough" } },
    };
    const taskSpec = {
      details: { inputs: null, kind: "agent" },
      env: {},
      kind: "agent",
      secret_env: {},
      task_id: TASK_ID,
      timeout_seconds: 60,
    };
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(sessionResponse, 201))
      .mockResolvedValueOnce(jsonResponse(sessionResponse))
      .mockResolvedValueOnce(jsonResponse([nodeResponse]))
      .mockResolvedValueOnce(jsonResponse(replayResponse))
      .mockResolvedValueOnce(jsonResponse(taskSpec))
      .mockResolvedValueOnce(jsonResponse({ found: true, result: null }));
    const client = new KitaruClient({
      apiKey: "secret",
      apiUrl: "https://api.example",
      fetch,
    });
    const update = { outputs: { ok: true }, status: "completed" } as const;
    const nodes = { nodes: [] } as SessionNodeBatchRequest;
    const lookup = {
      cache_key: "a".repeat(64),
      tool_name: "normalize",
    } satisfies ToolLookupRequest;

    await client.createSession(createRequest);
    await client.updateSession(SESSION_ID, update);
    await client.upsertSessionNodes(SESSION_ID, nodes);
    await client.getReplay(REPLAY_ID);
    await client.getTaskSpec(TASK_ID);
    await client.lookupToolResult(REPLAY_ID, lookup);

    expect(
      fetch.mock.calls.map(([url, init]) => ({
        authorization: (init?.headers as Record<string, string>).Authorization,
        body: init?.body,
        contentType: (init?.headers as Record<string, string>)["Content-Type"],
        method: init?.method,
        url,
      })),
    ).toEqual([
      {
        authorization: "Bearer secret",
        body: JSON.stringify(createRequest),
        contentType: "application/json",
        method: "POST",
        url: "https://api.example/api/v1/sessions",
      },
      {
        authorization: "Bearer secret",
        body: JSON.stringify(update),
        contentType: "application/json",
        method: "PATCH",
        url: `https://api.example/api/v1/sessions/${SESSION_ID}`,
      },
      {
        authorization: "Bearer secret",
        body: JSON.stringify(nodes),
        contentType: "application/json",
        method: "POST",
        url: `https://api.example/api/v1/sessions/${SESSION_ID}/nodes`,
      },
      {
        authorization: "Bearer secret",
        body: undefined,
        contentType: undefined,
        method: "GET",
        url: `https://api.example/api/v1/replays/${REPLAY_ID}`,
      },
      {
        authorization: "Bearer secret",
        body: undefined,
        contentType: undefined,
        method: "GET",
        url: `https://api.example/api/v1/tasks/${TASK_ID}/spec`,
      },
      {
        authorization: "Bearer secret",
        body: JSON.stringify(lookup),
        contentType: "application/json",
        method: "POST",
        url: `https://api.example/api/v1/replays/${REPLAY_ID}/tool-lookup`,
      },
    ]);
  });
});
