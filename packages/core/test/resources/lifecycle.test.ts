import { describe, expect, it, vi } from "vitest";

import {
  type ExperimentCreateRequest,
  type ExperimentRunCreateRequest,
  KitaruApiError,
  KitaruClient,
  KitaruWaitError,
  type ReplayCreateRequest,
} from "../../src/index.js";

const ID = "018f0000-0000-7000-8000-000000000001";
const OWNER_ID = "018f0000-0000-7000-8000-000000000002";
const RELATED_ID = "018f0000-0000-7000-8000-000000000003";
const RESULT_ID = "018f0000-0000-7000-8000-000000000004";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const toolPolicy = {
  default: { on_miss: "fail", scope: "baseline", type: "history" },
  tools: {},
};

const experiment = {
  agent_id: RELATED_ID,
  created: "2026-01-01T00:00:00Z",
  description: null,
  evaluators: [{ evaluator: "correctness" }],
  id: ID,
  name: "support-regression",
  override: null,
  owner_id: OWNER_ID,
  tool_policy: toolPolicy,
  updated: "2026-01-01T00:00:00Z",
};

const progress = {
  canceled: 0,
  completed: 0,
  evaluating: 0,
  failed: 0,
  pending: 1,
  total: 1,
};

const experimentRun = {
  agent_version_id: RELATED_ID,
  cohort_version_id: RESULT_ID,
  created: "2026-01-01T00:00:00Z",
  evaluate_baselines: false,
  experiment_id: ID,
  id: RELATED_ID,
  number: 1,
  owner_id: OWNER_ID,
  progress,
  status: "running",
  updated: "2026-01-01T00:00:00Z",
};

const job = {
  created: "2026-01-01T00:00:00Z",
  id: ID,
  kind: "replay",
  owner_id: OWNER_ID,
  status: "pending",
  updated: "2026-01-01T00:00:00Z",
};

const task = {
  agent_version_id: RELATED_ID,
  attempt: 1,
  created: "2026-01-01T00:00:00Z",
  id: RELATED_ID,
  job_id: ID,
  kind: "agent",
  labels: {},
  on_failure: "abort",
  result: null,
  result_session_id: RESULT_ID,
  status: "completed",
  updated: "2026-01-01T00:00:00Z",
};

const taskSpec = {
  details: { inputs: { ticket_id: "ticket-004" }, kind: "agent" },
  env: {},
  kind: "agent",
  run: { command: "node dist/main.js", env: {}, working_dir: null },
  secret_env: {},
  task_id: RELATED_ID,
  timeout_seconds: 180,
};

const replay = {
  baseline_session_id: RELATED_ID,
  created: "2026-01-01T00:00:00Z",
  evaluate_baselines: false,
  evaluators: [{ evaluator: "correctness" }],
  experiment_run_id: null,
  id: ID,
  job_id: OWNER_ID,
  override: null,
  result_session_id: RESULT_ID,
  status: "pending",
  tool_policy: toolPolicy,
  updated: "2026-01-01T00:00:00Z",
};

describe("execution lifecycle resources", () => {
  it("implements experiment and experiment-run routes", async () => {
    const createRequest = {
      agent_id: RELATED_ID,
      evaluators: [{ evaluator: "correctness" }],
      name: "support-regression",
    } satisfies ExperimentCreateRequest;
    const runRequest = {
      agent_version_id: RELATED_ID,
      cohort_version_id: RESULT_ID,
      evaluate_baselines: false,
    } satisfies ExperimentRunCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(experiment, 201))
      .mockResolvedValueOnce(jsonResponse(experiment))
      .mockResolvedValueOnce(jsonResponse({ ...experiment, name: "renamed" }))
      .mockResolvedValueOnce(
        jsonResponse({ items: [experiment], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse(experimentRun, 201))
      .mockResolvedValueOnce(jsonResponse(experimentRun))
      .mockResolvedValueOnce(
        jsonResponse({ items: [experimentRun], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse({ items: [job], next_cursor: null }))
      .mockResolvedValueOnce(
        jsonResponse({ ...experimentRun, status: "canceling" }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.experiments.create(createRequest);
    await client.experiments.get(ID);
    await client.experiments.update(ID, { name: "renamed" });
    await client.experiments.list({ size: 1 });
    await client.experiments.startRun(ID, runRequest);
    await client.experimentRuns.get(RELATED_ID);
    await client.experimentRuns.list({ sort: "created:asc" });
    await client.experimentRuns.listJobs(RELATED_ID, { size: 10 });
    await client.experimentRuns.cancel(RELATED_ID);
    await client.experimentRuns.delete(RELATED_ID);
    await client.experiments.delete(ID);

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ["https://api.example/v1/experiments", "POST"],
      [`https://api.example/v1/experiments/${ID}`, "GET"],
      [`https://api.example/v1/experiments/${ID}`, "PATCH"],
      ["https://api.example/v1/experiments?size=1", "GET"],
      [`https://api.example/v1/experiments/${ID}/runs`, "POST"],
      [`https://api.example/v1/experiment-runs/${RELATED_ID}`, "GET"],
      ["https://api.example/v1/experiment-runs?sort=created%3Aasc", "GET"],
      [
        `https://api.example/v1/experiment-runs/${RELATED_ID}/jobs?size=10`,
        "GET",
      ],
      [`https://api.example/v1/experiment-runs/${RELATED_ID}/cancel`, "POST"],
      [`https://api.example/v1/experiment-runs/${RELATED_ID}`, "DELETE"],
      [`https://api.example/v1/experiments/${ID}`, "DELETE"],
    ]);
  });

  it("implements job, task recovery, and replay routes without worker mutations", async () => {
    const replayRequest = {
      baseline_session_id: RELATED_ID,
      evaluate_baselines: false,
      evaluators: [{ evaluator: "correctness" }],
    } satisfies ReplayCreateRequest;
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse(job))
      .mockResolvedValueOnce(jsonResponse({ items: [job], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse({ items: [task], next_cursor: null }))
      .mockResolvedValueOnce(
        jsonResponse({ ...job, cancel_requested_at: "now" }),
      )
      .mockResolvedValueOnce(jsonResponse(task))
      .mockResolvedValueOnce(jsonResponse(taskSpec))
      .mockResolvedValueOnce(jsonResponse({ items: [task], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse(replay, 201))
      .mockResolvedValueOnce(jsonResponse(replay))
      .mockResolvedValueOnce(
        jsonResponse({ items: [replay], next_cursor: null }),
      )
      .mockResolvedValueOnce(jsonResponse({ found: false, result: null }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await client.jobs.get(ID);
    await client.jobs.list({
      filter: { field: "kind", op: "eq", value: "replay" },
    });
    await client.jobs.listTasks(ID);
    await client.jobs.cancel(ID);
    await client.tasks.get(RELATED_ID);
    await client.getTaskSpec(RELATED_ID);
    await client.tasks.list({ size: 1 });
    await client.replays.create(replayRequest);
    await client.getReplay(ID);
    await client.replays.list({ size: 1 });
    await client.lookupToolResult(ID, {
      cache_key: "a".repeat(64),
      tool_name: "lookup_order",
    });
    await client.jobs.delete(ID);

    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      [`https://api.example/v1/jobs/${ID}`, "GET"],
      [
        `https://api.example/v1/jobs?filter=${encodeURIComponent(JSON.stringify({ field: "kind", op: "eq", value: "replay" }))}`,
        "GET",
      ],
      [`https://api.example/v1/jobs/${ID}/tasks`, "GET"],
      [`https://api.example/v1/jobs/${ID}/cancel`, "POST"],
      [`https://api.example/v1/tasks/${RELATED_ID}`, "GET"],
      [`https://api.example/v1/tasks/${RELATED_ID}/spec`, "GET"],
      ["https://api.example/v1/tasks?size=1", "GET"],
      ["https://api.example/v1/replays", "POST"],
      [`https://api.example/v1/replays/${ID}`, "GET"],
      ["https://api.example/v1/replays?size=1", "GET"],
      [`https://api.example/v1/replays/${ID}/tool-lookup`, "POST"],
      [`https://api.example/v1/jobs/${ID}`, "DELETE"],
    ]);
    expect("claim" in client.tasks).toBe(false);
    expect("update" in client.tasks).toBe(false);
    expect("cancel" in client.replays).toBe(false);
    expect("delete" in client.replays).toBe(false);
  });

  it.each([
    ["completed", "jobs"],
    ["failed", "jobs"],
    ["canceled", "jobs"],
  ] as const)("returns terminal %s jobs without remapping", async (status, _resource) => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ ...job, status: "running" }))
      .mockResolvedValueOnce(jsonResponse({ ...job, status }));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.jobs.wait(ID, { intervalMs: 1, timeoutMs: 100 }),
    ).resolves.toMatchObject({ id: ID, status });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("treats run canceling and replay evaluating as nonterminal", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        jsonResponse({ ...experimentRun, status: "canceling" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          ...experimentRun,
          progress: { ...progress, canceled: 1, pending: 0 },
          status: "canceled",
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ ...replay, status: "evaluating" }))
      .mockResolvedValueOnce(
        jsonResponse({ ...replay, error: "scorer failed", status: "failed" }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(
      client.experimentRuns.wait(RELATED_ID, {
        intervalMs: 1,
        timeoutMs: 100,
      }),
    ).resolves.toMatchObject({ status: "canceled" });
    await expect(
      client.replays.wait(ID, { intervalMs: 1, timeoutMs: 100 }),
    ).resolves.toMatchObject({ error: "scorer failed", status: "failed" });
    expect(fetch).toHaveBeenCalledTimes(4);
  });

  it("validates wait settings before fetching", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    await expect(client.jobs.wait(ID, { intervalMs: 0 })).rejects.toThrow(
      "intervalMs",
    );
    await expect(
      client.replays.wait(ID, { timeoutMs: Number.NaN }),
    ).rejects.toThrow("timeoutMs");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("times out locally with the last state while remote work continues", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ ...job, status: "running" }))
      .mockImplementationOnce(
        (_url, init) =>
          new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(
              "abort",
              () =>
                reject(
                  init.signal?.reason ??
                    new DOMException("Aborted", "AbortError"),
                ),
              { once: true },
            );
          }),
      );
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const error = await client.jobs
      .wait(ID, { intervalMs: 1, timeoutMs: 15 })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruWaitError);
    expect(error).toMatchObject({
      kind: "timeout",
      lastState: expect.objectContaining({ status: "running" }),
      remoteContinues: true,
      resourceId: ID,
    });
    expect((error as KitaruWaitError<unknown>).toJSON()).toMatchObject({
      lastStatus: "running",
      remoteContinues: true,
      resourceId: ID,
    });
  });

  it("preserves a transport timeout before the wait deadline", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(
      (_url, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () =>
              reject(
                init.signal?.reason ??
                  new DOMException("Aborted", "AbortError"),
              ),
            { once: true },
          );
        }),
    );
    const client = new KitaruClient({
      apiUrl: "https://api.example",
      fetch,
      timeoutMs: 5,
    });

    const error = await client.jobs
      .wait(ID, { intervalMs: 1, timeoutMs: 1_000 })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruApiError);
    expect(error).not.toBeInstanceOf(KitaruWaitError);
    expect(error).toMatchObject({ kind: "timeout", status: null });
    expect((error as Error).message).toContain("timed out after 5ms");
  });

  it("caller abort stops only local waiting and retains the last state", async () => {
    const controller = new AbortController();
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementation(async () => {
        controller.abort("stop");
        return jsonResponse({ ...replay, status: "evaluating" });
      });
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const error = await client.replays
      .wait(ID, {
        intervalMs: 1,
        signal: controller.signal,
        timeoutMs: 100,
      })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruWaitError);
    expect(error).toMatchObject({
      kind: "canceled",
      lastState: expect.objectContaining({ status: "evaluating" }),
      remoteContinues: true,
      resourceId: ID,
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("keeps raw cancellation to one POST and surfaces 409 unchanged", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(jsonResponse({ detail: "already settled" }, 409));
    const client = new KitaruClient({ apiUrl: "https://api.example", fetch });

    const error = await client.jobs
      .cancel(ID)
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KitaruApiError);
    expect(error).toMatchObject({ status: 409 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
