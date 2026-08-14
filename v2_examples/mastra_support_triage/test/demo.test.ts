import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { KitaruClient, ReplayResponse } from "@zenml-io/kitaru";
import { describe, expect, it, vi } from "vitest";

import { parseArguments, runDemo } from "../src/demo.js";
import { operationFingerprint, RunManifestStore } from "../src/run-state.js";

const RUN_ID = "018f0000-0000-7000-8000-000000000030";
const OWNER_ID = "018f0000-0000-7000-8000-000000000031";
const INITIAL_SESSION_ID = "018f0000-0000-7000-8000-000000000032";
const REPLAY_ID = "018f0000-0000-7000-8000-000000000033";
const REPLAY_JOB_ID = "018f0000-0000-7000-8000-000000000034";
const RESULT_SESSION_ID = "018f0000-0000-7000-8000-000000000035";
const AGENT_ID = "018f0000-0000-7000-8000-000000000036";
const AGENT_VERSION_ID = "018f0000-0000-7000-8000-000000000037";
const EVALUATOR_ID = "018f0000-0000-7000-8000-000000000038";
const EVALUATOR_BLOB_ID = "018f0000-0000-7000-8000-000000000039";
const EVALUATOR_VERSION_ID = "018f0000-0000-7000-8000-000000000040";
const INITIAL_JOB_ID = "018f0000-0000-7000-8000-000000000041";
const INITIAL_REQUEST = {
  agent_version_id: AGENT_VERSION_ID,
  inputs:
    "Investigate account acct-1001 and delayed order ord-1001. " +
    "The customer reports a suspected duplicate charge.",
  name: "Mastra support triage baseline",
};

describe("runDemo recovery", () => {
  it("fails worker authentication before creating remote resources", async () => {
    const createAgent = vi.fn();
    const preflightWorker = vi
      .fn()
      .mockRejectedValue(new Error("worker authentication failed"));
    const fakeClient = {
      accounts: { getCurrent: vi.fn().mockResolvedValue({ id: OWNER_ID }) },
      agents: { create: createAgent },
    } as unknown as KitaruClient;

    await expect(
      runDemo(
        {
          apiUrl: "https://kitaru.example.test",
          stateRoot: await mkdtemp(join(tmpdir(), "kitaru-mastra-preflight-")),
          testModel: true,
        },
        { client: fakeClient, preflightWorker },
      ),
    ).rejects.toThrow("worker authentication failed");

    expect(preflightWorker).toHaveBeenCalledWith({
      apiUrl: "https://kitaru.example.test",
    });
    expect(createAgent).not.toHaveBeenCalled();
  });

  it("parses one explicit adoption or retry recovery action", () => {
    expect(
      parseArguments([
        "--resume",
        "/tmp/run",
        "--adopt",
        `create_agent=${REPLAY_ID}`,
      ]),
    ).toMatchObject({
      adoptions: { create_agent: REPLAY_ID },
      resumeStateDir: "/tmp/run",
    });
    expect(parseArguments(["--retry", "create_replay"]).retries).toEqual(
      new Set(["create_replay"]),
    );
    expect(parseArguments(["--", "--resume", "/tmp/run"]).resumeStateDir).toBe(
      "/tmp/run",
    );
    expect(() =>
      parseArguments(["--adopt", "create_agent=not-a-uuid"]),
    ).toThrow("operation=UUID");
    expect(() =>
      parseArguments([
        "--adopt",
        `create_agent=${REPLAY_ID}`,
        "--retry",
        "create_agent",
      ]),
    ).toThrow("more than once");
  });

  it("rejects a concurrent resume before calling the server", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-concurrent-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    let releaseFirst!: () => void;
    const firstRun = store.withRunLock(
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
    );
    await vi.waitFor(() => expect(releaseFirst).toBeTypeOf("function"));
    const getCurrent = vi.fn();
    const fakeClient = {
      accounts: { getCurrent },
    } as unknown as KitaruClient;

    await expect(
      runDemo(
        {
          apiUrl: "https://kitaru.example.test",
          resumeStateDir: store.stateDir,
          testModel: true,
        },
        { client: fakeClient },
      ),
    ).rejects.toThrow("already active");
    expect(getCurrent).not.toHaveBeenCalled();

    releaseFirst();
    await firstRun;
  });

  it("returns a completed manifest without creating or executing paid work again", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-completed-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const manifest = await store.read();
    manifest.resources = {
      initial_session_id: INITIAL_SESSION_ID,
      replay_id: REPLAY_ID,
      replay_job_id: REPLAY_JOB_ID,
      result_session_id: RESULT_SESSION_ID,
    };
    manifest.status = "completed";
    manifest.summary = {
      initial_outbox_count: 1,
      replay_outbox_count: 1,
    };
    await store.save(manifest);

    const createAgent = vi.fn();
    const createReplay = vi.fn();
    const runWorker = vi.fn();
    const replay = {
      baseline_session_id: INITIAL_SESSION_ID,
      created: "2026-08-14T10:00:00Z",
      evaluate_baselines: false,
      evaluators: [],
      id: REPLAY_ID,
      job_id: REPLAY_JOB_ID,
      override: null,
      result_session_id: RESULT_SESSION_ID,
      status: "completed",
      tool_policy: { default: { type: "passthrough" } },
      updated: "2026-08-14T10:00:01Z",
    } satisfies ReplayResponse;
    const fakeClient = {
      accounts: { getCurrent: vi.fn().mockResolvedValue({ id: OWNER_ID }) },
      agents: { create: createAgent },
      evaluations: {
        iter: async function* () {},
      },
      replays: { create: createReplay, get: vi.fn().mockResolvedValue(replay) },
      sessions: {
        iterNodes: async function* () {},
      },
    } as unknown as KitaruClient;

    const result = await runDemo(
      {
        apiUrl: "https://kitaru.example.test",
        resumeStateDir: store.stateDir,
        testModel: true,
      },
      { client: fakeClient, runWorker },
    );

    expect(result).toMatchObject({
      initial_outbox_count: 1,
      initial_session_id: INITIAL_SESSION_ID,
      replay_outbox_count: 1,
      result_session_id: RESULT_SESSION_ID,
    });
    expect(createAgent).not.toHaveBeenCalled();
    expect(createReplay).not.toHaveBeenCalled();
    expect(runWorker).not.toHaveBeenCalled();
  });

  it("forwards an explicit retry to an ambiguous paid create", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-retry-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const manifest = await store.read();
    manifest.resources = {
      agent_id: AGENT_ID,
      agent_version_id: AGENT_VERSION_ID,
      evaluator_blob_id: EVALUATOR_BLOB_ID,
      evaluator_id: EVALUATOR_ID,
      evaluator_version_id: EVALUATOR_VERSION_ID,
    };
    manifest.operations.push({
      fingerprint: operationFingerprint(INITIAL_REQUEST),
      kind: "create_initial_job",
      remote_ids: [],
      started_at: "2026-08-14T10:00:00.000Z",
      state: "ambiguous",
    });
    await store.save(manifest);

    const initialJob = {
      cancel_requested_at: null,
      created: "2026-08-14T10:00:00Z",
      ended_at: null,
      error: null,
      id: INITIAL_JOB_ID,
      kind: "session_run",
      owner_id: OWNER_ID,
      started_at: null,
      status: "pending",
      updated: "2026-08-14T10:00:00Z",
    } as const;
    const createInitialJob = vi.fn().mockResolvedValue(initialJob);
    const fakeClient = {
      accounts: { getCurrent: vi.fn().mockResolvedValue({ id: OWNER_ID }) },
      jobs: {
        cancel: vi.fn().mockResolvedValue({
          ...initialJob,
          cancel_requested_at: "2026-08-14T10:00:01Z",
        }),
        get: vi.fn().mockImplementation((id: string) => ({
          ...initialJob,
          id,
        })),
        listTasks: vi.fn().mockResolvedValue({
          items: [
            {
              agent_version_id: AGENT_VERSION_ID,
              job_id: INITIAL_JOB_ID,
              kind: "agent",
            },
          ],
          next_cursor: null,
        }),
        wait: vi.fn(),
      },
      sessionRuns: { create: createInitialJob },
    } as unknown as KitaruClient;

    await expect(
      runDemo(
        {
          apiUrl: "https://kitaru.example.test",
          resumeStateDir: store.stateDir,
          retries: new Set(["create_initial_job"]),
          testModel: true,
        },
        {
          client: fakeClient,
          runWorker: vi
            .fn()
            .mockRejectedValue(new Error("stop after paid create")),
        },
      ),
    ).rejects.toThrow(
      `Dedicated worker failed for Kitaru job ${INITIAL_JOB_ID}`,
    );

    expect(createInitialJob).toHaveBeenCalledTimes(1);
    expect(createInitialJob).toHaveBeenCalledWith(INITIAL_REQUEST);
    expect((await store.read()).operations).toMatchObject([
      { kind: "create_initial_job", state: "retried" },
      {
        kind: "create_initial_job",
        remote_ids: [INITIAL_JOB_ID],
        state: "committed",
      },
    ]);
  });
});
