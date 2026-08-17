import { describe, expect, it, vi } from "vitest";

import {
  type AnnotationCreateRequest,
  type CohortCreateRequest,
  type EvaluationBatchCreateRequest,
  type EvaluatorCreateRequest,
  type InvestigationCreateRequest,
  KitaruClient,
} from "../../src/index.js";

const ID = "018f0000-0000-7000-8000-000000000001";
const OWNER_ID = "018f0000-0000-7000-8000-000000000002";
const RELATED_ID = "018f0000-0000-7000-8000-000000000003";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const investigation = {
  agent_id: RELATED_ID,
  completed_sessions: 0,
  created: "2026-01-01T00:00:00Z",
  description: null,
  id: ID,
  metadata: {},
  name: "Support review",
  owner_id: OWNER_ID,
  status: "pending",
  total_sessions: 1,
  updated: "2026-01-01T00:00:00Z",
};

const investigationSession = {
  created: "2026-01-01T00:00:00Z",
  id: RELATED_ID,
  investigation_id: ID,
  position: 0,
  questions: [{ key: "correct", question: "Was the answer correct?" }],
  session_id: OWNER_ID,
  updated: "2026-01-01T00:00:00Z",
  verdict: null,
};

const annotation = {
  created: "2026-01-01T00:00:00Z",
  id: ID,
  owner_id: OWNER_ID,
  session_id: RELATED_ID,
  updated: "2026-01-01T00:00:00Z",
  value: "correct",
};

const evaluator = {
  created: "2026-01-01T00:00:00Z",
  description: null,
  id: ID,
  latest_version: 1,
  logo_url: null,
  metadata: {},
  name: "correctness",
  owner_id: OWNER_ID,
  updated: "2026-01-01T00:00:00Z",
};

const evaluatorVersion = {
  created: "2026-01-01T00:00:00Z",
  display_version: null,
  evaluator_id: ID,
  id: RELATED_ID,
  source: { blob_id: OWNER_ID, entrypoint: "evaluate", type: "script" },
  updated: "2026-01-01T00:00:00Z",
  version: 1,
};

const evaluation = {
  created: "2026-01-01T00:00:00Z",
  data_type: "float",
  id: ID,
  name: "correctness",
  owner_id: OWNER_ID,
  score: 0.9,
  session_id: RELATED_ID,
  updated: "2026-01-01T00:00:00Z",
};

const job = {
  created: "2026-01-01T00:00:00Z",
  id: ID,
  kind: "evaluation",
  owner_id: OWNER_ID,
  status: "pending",
  updated: "2026-01-01T00:00:00Z",
};

const cohort = {
  agent_id: RELATED_ID,
  created: "2026-01-01T00:00:00Z",
  description: null,
  id: ID,
  latest_version: 1,
  metadata: {},
  name: "regression",
  owner_id: OWNER_ID,
  updated: "2026-01-01T00:00:00Z",
};

const cohortVersion = {
  cohort_id: ID,
  created: "2026-01-01T00:00:00Z",
  display_version: null,
  id: RELATED_ID,
  owner_id: OWNER_ID,
  session_count: 1,
  updated: "2026-01-01T00:00:00Z",
  version: 1,
};

describe("management resources", () => {
  it("implements the investigation and investigation-session contract", async () => {
    const createRequest = {
      agent_id: RELATED_ID,
      name: "Support review",
      sessions: [
        {
          questions: [{ key: "correct", question: "Was it correct?" }],
          session_id: OWNER_ID,
        },
      ],
    } satisfies InvestigationCreateRequest;
    const controller = new AbortController();
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(investigation, 201))
      .mockResolvedValueOnce(jsonResponse(investigation))
      .mockResolvedValueOnce(
        jsonResponse({ ...investigation, status: "completed" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [investigation], next_cursor: null }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [investigationSession], next_cursor: "next" }),
      )
      .mockResolvedValueOnce(jsonResponse({ items: [], next_cursor: null }))
      .mockResolvedValueOnce(
        jsonResponse({ ...investigationSession, verdict: "acceptable" }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.investigations.create(createRequest, {
      signal: controller.signal,
    });
    await client.investigations.get(ID);
    await client.investigations.update(ID, { status: "completed" });
    await client.investigations.list({
      filter: { field: "status", op: "eq", value: "pending" },
    });
    const sessions = [];
    for await (const item of client.investigations.iterSessions(ID, {
      size: 1,
    })) {
      sessions.push(item);
    }
    await client.investigations.updateSession(ID, OWNER_ID, {
      verdict: "acceptable",
    });
    await expect(client.investigations.delete(ID)).resolves.toBeUndefined();

    expect(sessions).toEqual([investigationSession]);
    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/investigations", "POST"],
      [`https://api.example/api/v1/investigations/${ID}`, "GET"],
      [`https://api.example/api/v1/investigations/${ID}`, "PATCH"],
      [
        `https://api.example/api/v1/investigations?filter=${encodeURIComponent(JSON.stringify({ field: "status", op: "eq", value: "pending" }))}`,
        "GET",
      ],
      [
        `https://api.example/api/v1/investigations/${ID}/sessions?size=1`,
        "GET",
      ],
      [
        `https://api.example/api/v1/investigations/${ID}/sessions?cursor=next&size=1`,
        "GET",
      ],
      [
        `https://api.example/api/v1/investigations/${ID}/sessions/${OWNER_ID}`,
        "PATCH",
      ],
      [`https://api.example/api/v1/investigations/${ID}`, "DELETE"],
    ]);
    expect(fetch.mock.calls[0]?.[1]?.signal).toBeDefined();
    expect(fetch.mock.calls[0]?.[1]?.signal?.aborted).toBe(false);
    expect(fetch.mock.calls[0]?.[1]?.body).toBe(JSON.stringify(createRequest));
  });

  it("implements annotation CRUD and cursor iteration", async () => {
    const request = {
      session_id: RELATED_ID,
      value: "correct",
    } satisfies AnnotationCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(annotation, 201))
      .mockResolvedValueOnce(jsonResponse(annotation))
      .mockResolvedValueOnce(jsonResponse({ ...annotation, value: "fixed" }))
      .mockResolvedValueOnce(
        jsonResponse({ items: [annotation], next_cursor: "opaque+/=" }),
      )
      .mockResolvedValueOnce(jsonResponse({ items: [], next_cursor: null }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.annotations.create(request);
    await client.annotations.get(ID);
    await client.annotations.update(ID, { value: "fixed" });
    for await (const _item of client.annotations.iter({ size: 1 })) {
      // Exercise cursor iteration.
    }
    await expect(client.annotations.delete(ID)).resolves.toBeUndefined();

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/annotations", "POST"],
      [`https://api.example/api/v1/annotations/${ID}`, "GET"],
      [`https://api.example/api/v1/annotations/${ID}`, "PATCH"],
      ["https://api.example/api/v1/annotations?size=1", "GET"],
      [
        "https://api.example/api/v1/annotations?cursor=opaque%2B%2F%3D&size=1",
        "GET",
      ],
      [`https://api.example/api/v1/annotations/${ID}`, "DELETE"],
    ]);
  });

  it("implements evaluator CRUD and its number-addressed version contract", async () => {
    const request = { name: "correctness" } satisfies EvaluatorCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(evaluator, 201))
      .mockResolvedValueOnce(jsonResponse(evaluator))
      .mockResolvedValueOnce(jsonResponse({ ...evaluator, description: "new" }))
      .mockResolvedValueOnce(
        jsonResponse({ items: [evaluator], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse(evaluatorVersion, 201))
      .mockResolvedValueOnce(jsonResponse(evaluatorVersion))
      .mockResolvedValueOnce(
        jsonResponse({ ...evaluatorVersion, display_version: "v1" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ items: [evaluatorVersion], next_cursor: null }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.evaluators.create(request);
    await client.evaluators.get(ID);
    await client.evaluators.update(ID, { description: "new" });
    await client.evaluators.list({ sort: "name:asc" });
    await client.evaluators.createVersion(ID, {
      source: { blob_id: OWNER_ID, entrypoint: "evaluate", type: "script" },
    });
    await client.evaluators.getVersion(ID, 1);
    await client.evaluators.updateVersion(ID, 1, { display_version: "v1" });
    for await (const _item of client.evaluators.iterVersions(ID, {
      size: 10,
      sort: "version:desc",
    })) {
      // Exercise the unfiltered version list contract.
    }
    await expect(client.evaluators.delete(ID)).resolves.toBeUndefined();

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/evaluators", "POST"],
      [`https://api.example/api/v1/evaluators/${ID}`, "GET"],
      [`https://api.example/api/v1/evaluators/${ID}`, "PATCH"],
      ["https://api.example/api/v1/evaluators?sort=name%3Aasc", "GET"],
      [`https://api.example/api/v1/evaluators/${ID}/versions`, "POST"],
      [`https://api.example/api/v1/evaluators/${ID}/versions/1`, "GET"],
      [`https://api.example/api/v1/evaluators/${ID}/versions/1`, "PATCH"],
      [
        `https://api.example/api/v1/evaluators/${ID}/versions?size=10&sort=version%3Adesc`,
        "GET",
      ],
      [`https://api.example/api/v1/evaluators/${ID}`, "DELETE"],
    ]);
  });

  it("returns an evaluation job and reads evaluation results", async () => {
    const request = {
      evaluators: [{ evaluator: "correctness" }],
      input_session_ids: [RELATED_ID],
    } satisfies EvaluationBatchCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(job, 201))
      .mockResolvedValueOnce(jsonResponse(evaluation))
      .mockResolvedValueOnce(
        jsonResponse({ items: [evaluation], next_cursor: null }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.evaluations.create(request)).resolves.toEqual(job);
    await client.evaluations.get(ID);
    const results = [];
    for await (const item of client.evaluations.iter({ size: 1 })) {
      results.push(item);
    }

    expect(results).toEqual([evaluation]);
    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/evaluations", "POST"],
      [`https://api.example/api/v1/evaluations/${ID}`, "GET"],
      ["https://api.example/api/v1/evaluations?size=1", "GET"],
    ]);
  });

  it("implements cohort CRUD and separately addressed cohort versions", async () => {
    const request = {
      agent_id: RELATED_ID,
      name: "regression",
    } satisfies CohortCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(cohort, 201))
      .mockResolvedValueOnce(jsonResponse(cohort))
      .mockResolvedValueOnce(jsonResponse({ ...cohort, description: "new" }))
      .mockResolvedValueOnce(
        jsonResponse({ items: [cohort], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse(cohortVersion, 201))
      .mockResolvedValueOnce(
        jsonResponse({ items: [cohortVersion], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse(cohortVersion))
      .mockResolvedValueOnce(
        jsonResponse({ ...cohortVersion, display_version: "v1" }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.cohorts.create(request);
    await client.cohorts.get(ID);
    await client.cohorts.update(ID, { description: "new" });
    await client.cohorts.list();
    await client.cohorts.createVersion(ID, { add_session_ids: [OWNER_ID] });
    for await (const _item of client.cohorts.iterVersions(ID)) {
      // Exercise version iteration.
    }
    await client.cohortVersions.get(RELATED_ID);
    await client.cohortVersions.update(RELATED_ID, { display_version: "v1" });
    await expect(
      client.cohortVersions.delete(RELATED_ID),
    ).resolves.toBeUndefined();
    await expect(client.cohorts.delete(ID)).resolves.toBeUndefined();

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/api/v1/cohorts", "POST"],
      [`https://api.example/api/v1/cohorts/${ID}`, "GET"],
      [`https://api.example/api/v1/cohorts/${ID}`, "PATCH"],
      ["https://api.example/api/v1/cohorts", "GET"],
      [`https://api.example/api/v1/cohorts/${ID}/versions`, "POST"],
      [`https://api.example/api/v1/cohorts/${ID}/versions`, "GET"],
      [`https://api.example/api/v1/cohort-versions/${RELATED_ID}`, "GET"],
      [`https://api.example/api/v1/cohort-versions/${RELATED_ID}`, "PATCH"],
      [`https://api.example/api/v1/cohort-versions/${RELATED_ID}`, "DELETE"],
      [`https://api.example/api/v1/cohorts/${ID}`, "DELETE"],
    ]);
  });

  it("rejects invalid resource discriminators and identifiers", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ ...investigation, status: "stuck" }),
      )
      .mockResolvedValueOnce(jsonResponse({ ...evaluation, data_type: "json" }))
      .mockResolvedValueOnce(
        jsonResponse({ ...evaluatorVersion, source: { type: "wheel" } }),
      )
      .mockResolvedValueOnce(
        jsonResponse({ ...cohortVersion, cohort_id: "bad" }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.investigations.get(ID)).rejects.toThrow(
      "invalid status",
    );
    await expect(client.evaluations.get(ID)).rejects.toThrow(
      "invalid data_type",
    );
    await expect(client.evaluators.getVersion(ID, 1)).rejects.toThrow(
      "invalid source.type",
    );
    await expect(client.cohortVersions.get(RELATED_ID)).rejects.toThrow(
      "invalid cohort_id",
    );
  });
});
