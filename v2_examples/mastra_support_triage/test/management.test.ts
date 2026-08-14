import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { KitaruApiError } from "@zenml-io/kitaru";
import { describe, expect, it, vi } from "vitest";

import {
  type JobManagementClient,
  runOwnedJob,
  verifyReplaceableJob,
  WorkerJobError,
} from "../src/management.js";
import { RunManifestStore } from "../src/run-state.js";

const RUN_ID = "018f0000-0000-7000-8000-000000000020";
const OWNER_ID = "018f0000-0000-7000-8000-000000000021";
const JOB_ID = "018f0000-0000-7000-8000-000000000022";
const VERSION_ID = "018f0000-0000-7000-8000-000000000023";

function job(
  overrides: Partial<
    Awaited<ReturnType<JobManagementClient["jobs"]["get"]>>
  > = {},
) {
  return {
    cancel_requested_at: null,
    created: "2026-08-14T10:00:00Z",
    ended_at: null,
    error: null,
    id: JOB_ID,
    kind: "session_run" as const,
    owner_id: OWNER_ID,
    started_at: null,
    status: "pending" as const,
    updated: "2026-08-14T10:00:00Z",
    ...overrides,
  };
}

function task(
  overrides: Partial<
    Awaited<
      ReturnType<JobManagementClient["jobs"]["listTasks"]>
    >["items"][number]
  > = {},
) {
  return {
    agent_id: null,
    agent_version_id: VERSION_ID,
    attempt: 0,
    cancel_requested_at: null,
    claimed_at: null,
    created: "2026-08-14T10:00:00Z",
    ended_at: null,
    error: null,
    heartbeat_at: null,
    id: "018f0000-0000-7000-8000-000000000024",
    input_session_id: null,
    job_id: JOB_ID,
    kind: "agent" as const,
    labels: {},
    on_failure: "abort" as const,
    payload_blob_id: null,
    plugin_version_id: null,
    result: null,
    result_session_id: null,
    started_at: null,
    status: "pending" as const,
    updated: "2026-08-14T10:00:00Z",
    worker_id: null,
    ...overrides,
  };
}

async function setupStore(): Promise<RunManifestStore> {
  const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-management-"));
  return RunManifestStore.create({
    ownerId: OWNER_ID,
    rootDir: root,
    runId: RUN_ID,
    serverUrl: "https://kitaru.example.test",
  });
}

describe("runOwnedJob", () => {
  it("cancels once, reconciles a 409 by exact read, and preserves the worker error", async () => {
    const primary = new Error("worker authentication failed");
    const cancel = vi
      .fn()
      .mockRejectedValue(
        new KitaruApiError("POST", `/v1/jobs/${JOB_ID}/cancel`, 409, "race"),
      );
    const get = vi
      .fn()
      .mockResolvedValueOnce(job())
      .mockResolvedValueOnce(
        job({ cancel_requested_at: "2026-08-14T10:00:01Z" }),
      );
    const client: JobManagementClient = {
      jobs: {
        cancel,
        get,
        listTasks: vi.fn().mockResolvedValue({
          items: [task()],
          next_cursor: null,
        }),
        wait: vi.fn(),
      },
    };
    const store = await setupStore();

    const error = await runOwnedJob({
      client,
      expectedAgentVersionId: VERSION_ID,
      expectedKind: "session_run",
      jobId: JOB_ID,
      ownerId: OWNER_ID,
      runWorker: vi.fn().mockRejectedValue(primary),
      store,
    }).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(WorkerJobError);
    expect((error as WorkerJobError).cause).toBe(primary);
    expect((error as WorkerJobError).cancellation).toMatchObject({
      job_id: JOB_ID,
      state: "accepted",
      reconciled_after_error: true,
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(cancel).toHaveBeenCalledWith(JOB_ID);
    expect(get).toHaveBeenNthCalledWith(2, JOB_ID);
  });

  it("refuses to launch or cancel when the exact job is not owned by this run", async () => {
    const runWorker = vi.fn();
    const cancel = vi.fn();
    const client: JobManagementClient = {
      jobs: {
        cancel,
        get: vi
          .fn()
          .mockResolvedValue(
            job({ owner_id: "018f0000-0000-7000-8000-000000000099" }),
          ),
        listTasks: vi.fn(),
        wait: vi.fn(),
      },
    };

    await expect(
      runOwnedJob({
        client,
        expectedAgentVersionId: VERSION_ID,
        expectedKind: "session_run",
        jobId: JOB_ID,
        ownerId: OWNER_ID,
        runWorker,
        store: await setupStore(),
      }),
    ).rejects.toThrow("does not belong to account");

    expect(runWorker).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });

  it("refuses a job with the wrong workflow type or agent-version parent", async () => {
    const runWorker = vi.fn();
    const wrongKindClient: JobManagementClient = {
      jobs: {
        cancel: vi.fn(),
        get: vi.fn().mockResolvedValue(job({ kind: "replay" })),
        listTasks: vi.fn(),
        wait: vi.fn(),
      },
    };
    await expect(
      runOwnedJob({
        client: wrongKindClient,
        expectedAgentVersionId: VERSION_ID,
        expectedKind: "session_run",
        jobId: JOB_ID,
        ownerId: OWNER_ID,
        runWorker,
        store: await setupStore(),
      }),
    ).rejects.toThrow("has kind replay, expected session_run");

    const wrongParentClient: JobManagementClient = {
      jobs: {
        cancel: vi.fn(),
        get: vi.fn().mockResolvedValue(job()),
        listTasks: vi.fn().mockResolvedValue({
          items: [
            task({
              agent_version_id: "018f0000-0000-7000-8000-000000000099",
            }),
          ],
          next_cursor: null,
        }),
        wait: vi.fn(),
      },
    };
    await expect(
      runOwnedJob({
        client: wrongParentClient,
        expectedAgentVersionId: VERSION_ID,
        expectedKind: "session_run",
        jobId: JOB_ID,
        ownerId: OWNER_ID,
        runWorker,
        store: await setupStore(),
      }),
    ).rejects.toThrow("does not contain the expected agent task");
    expect(runWorker).not.toHaveBeenCalled();
  });

  it("does not run a worker again after the exact job completed", async () => {
    const runWorker = vi.fn();
    const completed = job({
      ended_at: "2026-08-14T10:00:03Z",
      status: "completed",
    });
    const client: JobManagementClient = {
      jobs: {
        cancel: vi.fn(),
        get: vi.fn().mockResolvedValue(completed),
        listTasks: vi.fn().mockResolvedValue({
          items: [task()],
          next_cursor: null,
        }),
        wait: vi.fn(),
      },
    };

    await expect(
      runOwnedJob({
        client,
        expectedAgentVersionId: VERSION_ID,
        expectedKind: "session_run",
        jobId: JOB_ID,
        ownerId: OWNER_ID,
        runWorker,
        store: await setupStore(),
      }),
    ).resolves.toEqual(completed);
    expect(runWorker).not.toHaveBeenCalled();
  });

  it("waits for cancellation already in progress without launching a worker", async () => {
    const runWorker = vi.fn();
    const canceled = job({
      cancel_requested_at: "2026-08-14T10:00:01Z",
      ended_at: "2026-08-14T10:00:03Z",
      status: "canceled",
    });
    const client: JobManagementClient = {
      jobs: {
        cancel: vi.fn(),
        get: vi
          .fn()
          .mockResolvedValue(
            job({ cancel_requested_at: "2026-08-14T10:00:01Z" }),
          ),
        listTasks: vi.fn().mockResolvedValue({
          items: [task()],
          next_cursor: null,
        }),
        wait: vi.fn().mockResolvedValue(canceled),
      },
    };

    await expect(
      runOwnedJob({
        client,
        expectedAgentVersionId: VERSION_ID,
        expectedKind: "session_run",
        jobId: JOB_ID,
        ownerId: OWNER_ID,
        runWorker,
        store: await setupStore(),
      }),
    ).rejects.toThrow(`settled as canceled`);

    expect(runWorker).not.toHaveBeenCalled();
    expect(client.jobs.wait).toHaveBeenCalledWith(JOB_ID);
  });
});

describe("verifyReplaceableJob", () => {
  it("accepts only an exact terminal failed or canceled job", async () => {
    const client: JobManagementClient = {
      jobs: {
        cancel: vi.fn(),
        get: vi
          .fn()
          .mockResolvedValueOnce(job({ status: "failed" }))
          .mockResolvedValueOnce(job({ status: "completed" })),
        listTasks: vi.fn().mockResolvedValue({
          items: [task()],
          next_cursor: null,
        }),
        wait: vi.fn(),
      },
    };
    const options = {
      client,
      expectedAgentVersionId: VERSION_ID,
      expectedKind: "session_run" as const,
      jobId: JOB_ID,
      ownerId: OWNER_ID,
    };

    await expect(verifyReplaceableJob(options)).resolves.toMatchObject({
      id: JOB_ID,
      status: "failed",
    });
    await expect(verifyReplaceableJob(options)).rejects.toThrow(
      "must be failed or canceled before replacement",
    );
  });
});
