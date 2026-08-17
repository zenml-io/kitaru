import { describe, expect, it, vi } from "vitest";

import {
  type AgentCreateRequest,
  type AgentVersionCreateRequest,
  type Filter,
  KitaruClient,
  type SessionCreateRequest,
} from "../../src/index.js";

const ID = "018f0000-0000-7000-8000-000000000001";
const OWNER_ID = "018f0000-0000-7000-8000-000000000002";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const agent = {
  created: "2026-01-01T00:00:00Z",
  description: null,
  id: ID,
  latest_version: 1,
  name: "support",
  owner_id: OWNER_ID,
  updated: "2026-01-01T00:00:00Z",
};

const version = {
  agent_id: ID,
  capabilities: {},
  created: "2026-01-01T00:00:00Z",
  description: null,
  display_version: null,
  id: ID,
  owner_id: OWNER_ID,
  run_spec: null,
  updated: "2026-01-01T00:00:00Z",
  version: 1,
};

const session = {
  id: ID,
  origin: "recorded",
  status: "completed",
};

describe("foundation resources", () => {
  it("gets unauthenticated server info and the current account", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ auth_scheme: "control_plane", version: "2.0.0" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          active: true,
          created: "2026-01-01T00:00:00Z",
          email: null,
          id: ID,
          is_admin: false,
          is_service_account: false,
          metadata: {},
          name: "Alex",
          updated: "2026-01-01T00:00:00Z",
        }),
      );
    const client = new KitaruClient({
      apiKey: "secret",
      apiUrl: "https://api.example",
      fetch,
    });

    await expect(client.info.get()).resolves.toMatchObject({
      version: "2.0.0",
    });
    await expect(client.accounts.getCurrent()).resolves.toMatchObject({
      id: ID,
    });

    const [, infoInit] = fetch.mock.calls[0] ?? [];
    const [, accountInit] = fetch.mock.calls[1] ?? [];
    expect(infoInit?.headers).not.toHaveProperty("Authorization");
    expect(accountInit?.headers).toMatchObject({
      Authorization: "Bearer secret",
    });
  });

  it("creates and reads agents and versions through typed routes", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(agent, 201))
      .mockResolvedValueOnce(jsonResponse(agent))
      .mockResolvedValueOnce(jsonResponse(version, 201))
      .mockResolvedValueOnce(jsonResponse(version));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });
    const agentRequest = { name: "support" } satisfies AgentCreateRequest;
    const versionRequest = {} satisfies AgentVersionCreateRequest;

    await client.agents.create(agentRequest);
    await client.agents.get(ID);
    await client.agents.createVersion(ID, versionRequest);
    await client.agents.getVersion(ID);

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/agents", "POST"],
      [`https://api.example/api/v1/agents/${ID}`, "GET"],
      [`https://api.example/api/v1/agents/${ID}/versions`, "POST"],
      [`https://api.example/api/v1/agent-versions/${ID}`, "GET"],
    ]);
    expect(await (fetch.mock.calls[0]?.[1]?.body as string)).toBe(
      JSON.stringify(agentRequest),
    );
  });

  it("updates and deletes agents and versions", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ ...agent, description: "updated" }))
      .mockResolvedValueOnce(
        jsonResponse({ ...version, display_version: "v2" }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.agents.update(ID, { description: "updated" });
    await client.agents.updateVersion(ID, { display_version: "v2" });
    await expect(client.agents.deleteVersion(ID)).resolves.toBeUndefined();
    await expect(client.agents.delete(ID)).resolves.toBeUndefined();

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      [`https://api.example/api/v1/agents/${ID}`, "PATCH"],
      [`https://api.example/api/v1/agent-versions/${ID}`, "PATCH"],
      [`https://api.example/api/v1/agent-versions/${ID}`, "DELETE"],
      [`https://api.example/api/v1/agents/${ID}`, "DELETE"],
    ]);
  });

  it("JSON-encodes recursive filters and iterates opaque cursors without mutation", async () => {
    const filter = {
      and: [
        { field: "name", op: "startswith", value: "support" },
        { not: { field: "status", op: "eq", value: "failed" } },
      ],
    } satisfies Filter;
    const params = { filter, size: 1, sort: "created:desc" } as const;
    const snapshot = structuredClone(params);
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "opaque+/=" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          items: [{ ...agent, name: "support-2" }],
          next_cursor: null,
        }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const names: string[] = [];
    for await (const item of client.agents.iter(params)) {
      names.push(item.name);
    }

    expect(names).toEqual(["support", "support-2"]);
    expect(params).toEqual(snapshot);
    const firstUrl = new URL(fetch.mock.calls[0]?.[0] as string);
    const secondUrl = new URL(fetch.mock.calls[1]?.[0] as string);
    expect(firstUrl.searchParams.get("filter")).toBe(JSON.stringify(filter));
    expect(firstUrl.searchParams.get("size")).toBe("1");
    expect(secondUrl.searchParams.get("cursor")).toBe("opaque+/=");
    expect(secondUrl.searchParams.get("filter")).toBe(JSON.stringify(filter));
  });

  it("stops when a malformed page repeats its cursor", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "stuck" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "stuck" }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const collect = async () => {
      for await (const _item of client.agents.iter({ cursor: "start" })) {
        // Consume the iterator until it either completes or rejects.
      }
    };

    await expect(collect()).rejects.toThrow("cursor did not advance");
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("stops when malformed pagination cycles through multiple cursors", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "cursor-a" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "cursor-b" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [agent], next_cursor: "cursor-a" }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const collect = async () => {
      for await (const _item of client.agents.iter({ cursor: "start" })) {
        // Consume the iterator until it either completes or rejects.
      }
    };

    await expect(collect()).rejects.toThrow("cursor did not advance");
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it("supports session CRUD reads, full payloads, node pages, and compatibility delegates", async () => {
    const node = {
      id: ID,
      node_type: "llm_call",
      status: "completed",
    };
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(session, 201))
      .mockResolvedValueOnce(jsonResponse(session))
      .mockResolvedValueOnce(jsonResponse(session))
      .mockResolvedValueOnce(jsonResponse({ session, nodes: [node] }))
      .mockResolvedValueOnce(jsonResponse({ items: [node], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse([node]))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });
    const request = {
      agent_id: ID,
      inputs: null,
      origin: "recorded",
      outputs: null,
    } satisfies SessionCreateRequest;

    await client.createSession(request);
    await client.sessions.get(ID);
    await client.updateSession(ID, { status: "completed" });
    await client.sessions.getWithNodes(ID);
    await client.sessions.listNodes(ID, { includePayloads: true });
    await client.upsertSessionNodes(ID, { nodes: [] });
    await expect(client.sessions.delete(ID)).resolves.toBeUndefined();

    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      "https://api.example/api/v1/sessions",
      `https://api.example/api/v1/sessions/${ID}`,
      `https://api.example/api/v1/sessions/${ID}`,
      `https://api.example/api/v1/sessions/${ID}/full`,
      `https://api.example/api/v1/sessions/${ID}/nodes?include_payloads=true`,
      `https://api.example/api/v1/sessions/${ID}/nodes`,
      `https://api.example/api/v1/sessions/${ID}`,
    ]);
  });

  it("creates a session run and validates its returned job", async () => {
    const job = {
      created: "2026-01-01T00:00:00Z",
      id: ID,
      kind: "session_run",
      owner_id: OWNER_ID,
      status: "pending",
      updated: "2026-01-01T00:00:00Z",
    };
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse(job, 201),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.sessionRuns.create({
        agent_version_id: ID,
        inputs: { ticket: 4 },
      }),
    ).resolves.toEqual(job);
    expect(fetch).toHaveBeenCalledWith(
      "https://api.example/api/v1/session-runs",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("rejects malformed foundation responses", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () =>
      jsonResponse({ items: "not-an-array", next_cursor: null }),
    );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.agents.list()).rejects.toThrow(
      "expected a page object",
    );
  });
});
