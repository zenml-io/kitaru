import { mkdtemp, readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  CohortVersionResponse,
  EvaluatorVersionResponse,
} from "@zenml-io/kitaru";
import { describe, expect, it, vi } from "vitest";

import {
  assertSupportedNodeVersion,
  validateWorkflowEnvironment,
} from "../src/preflight.js";
import {
  createCompletedEvent,
  createWorkerHandoffEvent,
} from "../src/worker-handoff.js";
import {
  createWorkflowManifest,
  loadWorkflowManifest,
  type WorkflowManifest,
  WorkflowManifestStore,
} from "../src/workflow-manifest.js";
import {
  BASELINE_FAILURE_TICKETS,
  CONTROL_TICKETS,
  getCohortVersionForAdoption,
  getEvaluatorVersionForAdoption,
  runJournaledMutation,
  TARGET_TICKETS,
  TOOLS,
  verifyCompletedJob,
  workflowRequests,
} from "../src/workflow-runner.js";

const id = (index: number) =>
  `018f0000-0000-7000-8000-${String(index).padStart(12, "0")}`;

function manifest(): WorkflowManifest {
  return createWorkflowManifest({
    accountId: id(1),
    apiUrl: "https://kitaru.example",
    authScheme: "control_plane",
    evidenceSetId: id(2),
    serverVersion: "0.22.0",
    sourceHashes: {
      baseline_instructions_sha256: "a".repeat(64),
      evaluator_sha256: "b".repeat(64),
      fixtures_sha256: "c".repeat(64),
      strict_instructions_sha256: "d".repeat(64),
    },
  });
}

describe("canonical workflow preflight", () => {
  it.each([
    "21.99.0",
    "22.21.9",
    "23.0.0",
    "not-a-version",
  ])("rejects unsupported Node %s", (version) => {
    expect(() => assertSupportedNodeVersion(version)).toThrow(
      "Node >=22.22.0 <23",
    );
  });

  it("accepts the pinned Node runtime", () => {
    expect(() => assertSupportedNodeVersion("22.22.3")).not.toThrow();
  });

  it("keeps the deterministic path provider-free", () => {
    expect(() =>
      validateWorkflowEnvironment("deterministic", {
        KITARU_API_URL: "https://kitaru.example",
      }),
    ).not.toThrow();
    expect(() =>
      validateWorkflowEnvironment("openai", {
        KITARU_API_URL: "https://kitaru.example",
        OPENAI_API_KEY: "secret",
      }),
    ).toThrow("RETURNS_ALLOW_PAID_MODEL=1");
  });
});

describe("canonical workflow manifest", () => {
  it("writes owner-only schema-v2 state atomically and reloads it", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const value = manifest();

    await store.save(value);

    expect(await loadWorkflowManifest(directory)).toEqual(value);
    expect((await stat(directory)).mode & 0o077).toBe(0);
    expect((await stat(join(directory, "workflow.json"))).mode & 0o077).toBe(0);
    expect(
      await readFile(join(directory, "workflow.json"), "utf8"),
    ).not.toContain("API_KEY");
  });

  it("rejects malformed and secret-bearing state", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const value = manifest();
    await expect(
      store.save({ ...value, api_key: "KITKEY_secret" } as WorkflowManifest),
    ).rejects.toThrow("allowlisted workflow manifest");

    await expect(
      store.save({
        ...value,
        provider: { ...value.provider, api_key: "KITKEY_secret" },
      } as WorkflowManifest),
    ).rejects.toThrow("allowlisted workflow manifest");
  });

  it("reuses an identical archive but rejects an evidence-set collision", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const value = manifest();

    await store.archive(value);
    await expect(store.archive(value)).resolves.toBeUndefined();

    await expect(
      store.archive({
        ...value,
        server: { ...value.server, version: "different" },
      }),
    ).rejects.toThrow("archive collision");
  });

  it("recovers one exact named mutation after a lost response", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const remote = { id: id(8), name: "exact-run-name" };
    const input = {
      commit: (current: WorkflowManifest, value: typeof remote) => {
        current.ids.agent_id = value.id;
      },
      create: async () => remote,
      fingerprintInput: { name: remote.name },
      key: "agent",
      kind: "agent",
      manifest: state,
      reconcile: async () => [remote],
      stage: "baseline" as const,
      store,
      validate: (value: typeof remote) => {
        if (value.name !== remote.name) {
          throw new Error("wrong remote");
        }
      },
    };

    await expect(
      runJournaledMutation(
        {
          ...input,
          afterRemoteCommit: async () => {
            throw new Error("lost response before manifest rename");
          },
        },
        { adoptions: {}, retries: new Set() },
      ),
    ).rejects.toThrow("lost response");
    expect((await store.load())?.pending_operation).toMatchObject({
      key: "agent",
      status: "submitted",
    });

    const recovered = await runJournaledMutation(input, {
      adoptions: {},
      retries: new Set(),
    });
    expect(recovered).toEqual(remote);
    expect((await store.load())?.ids.agent_id).toBe(remote.id);
    expect((await store.load())?.pending_operation).toBeNull();
  });

  it("stops a non-reconcilable response loss as ambiguous", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const remote = { id: id(9) };
    const input = {
      commit: (current: WorkflowManifest, value: typeof remote) => {
        current.ids.evaluation_job_id = value.id;
      },
      create: async () => remote,
      fingerprintInput: { input_session_ids: [id(20)] },
      key: "baseline_evaluation_job",
      kind: "evaluation_job",
      manifest: state,
      stage: "baseline_evaluation" as const,
      store,
      validate: () => {},
    };
    await expect(
      runJournaledMutation(
        {
          ...input,
          afterRemoteCommit: async () => {
            throw new Error("lost response");
          },
        },
        { adoptions: {}, retries: new Set() },
      ),
    ).rejects.toThrow("lost response");

    await expect(
      runJournaledMutation(input, { adoptions: {}, retries: new Set() }),
    ).rejects.toThrow("is ambiguous");
    expect((await store.load())?.pending_operation?.status).toBe("ambiguous");
  });

  it("adopts evaluator versions after response loss", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const remote: EvaluatorVersionResponse = {
      created: "2026-08-14T00:00:00Z",
      display_version: "1.0",
      id: id(30),
      evaluator_id: id(31),
      source: { blob_id: id(32), entrypoint: "evaluate", type: "script" },
      updated: "2026-08-14T00:00:00Z",
      version: 1,
    };
    const client = {
      evaluators: {
        iterVersions: vi.fn(async function* () {
          yield remote;
        }),
      },
    };
    const input = {
      adopt: (versionId: string) =>
        getEvaluatorVersionForAdoption(client, id(31), versionId),
      commit: (current: WorkflowManifest, value: typeof remote) => {
        current.ids.evaluator_version_id = value.id;
      },
      create: async () => remote,
      fingerprintInput: remote.source,
      key: "evaluator_version",
      kind: "evaluator_version",
      manifest: state,
      parentIds: { blob_id: id(32), evaluator_id: id(31) },
      stage: "baseline_evaluation" as const,
      store,
      validate: (value: typeof remote) => {
        if (
          value.evaluator_id !== id(31) ||
          value.source.type !== "script" ||
          value.source.blob_id !== id(32)
        ) {
          throw new Error("wrong evaluator version");
        }
      },
    };
    await expect(
      runJournaledMutation(
        {
          ...input,
          afterRemoteCommit: async () => {
            throw new Error("lost response");
          },
        },
        { adoptions: {}, retries: new Set() },
      ),
    ).rejects.toThrow("lost response");

    await expect(
      runJournaledMutation(input, {
        adoptions: { evaluator_version: remote.id },
        retries: new Set(),
      }),
    ).resolves.toEqual(remote);
    expect((await store.load())?.ids.evaluator_version_id).toBe(remote.id);
  });

  it("adopts cohort versions after response loss", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const remote: CohortVersionResponse = {
      cohort_id: id(41),
      created: "2026-08-14T00:00:00Z",
      display_version: "baseline-targets",
      id: id(40),
      owner_id: id(1),
      session_count: 2,
      updated: "2026-08-14T00:00:00Z",
      version: 1,
    };
    const client = {
      cohortVersions: { get: vi.fn(async () => remote) },
    };
    const input = {
      adopt: (versionId: string) =>
        getCohortVersionForAdoption(client, versionId),
      commit: (current: WorkflowManifest, value: typeof remote) => {
        current.ids.cohort_versions.target = value.id;
      },
      create: async () => remote,
      fingerprintInput: { display_version: "baseline-targets" },
      key: "cohort_version.target",
      kind: "cohort_version",
      manifest: state,
      parentIds: { cohort_id: id(41) },
      stage: "cohorts" as const,
      store,
      validate: (value: typeof remote) => {
        if (value.cohort_id !== id(41) || value.session_count !== 2) {
          throw new Error("wrong cohort version");
        }
      },
    };
    await expect(
      runJournaledMutation(
        {
          ...input,
          afterRemoteCommit: async () => {
            throw new Error("lost response");
          },
        },
        { adoptions: {}, retries: new Set() },
      ),
    ).rejects.toThrow("lost response");

    await expect(
      runJournaledMutation(input, {
        adoptions: { "cohort_version.target": remote.id },
        retries: new Set(),
      }),
    ).resolves.toEqual(remote);
    expect((await store.load())?.ids.cohort_versions.target).toBe(remote.id);
  });

  it.each([
    ["failed", "evaluation", "baseline_evaluation"],
    ["canceled", "replay", "experiment_runs"],
    ["completed", "replay", "baseline_evaluation"],
  ] as const)("persists failed state for %s %s jobs", async (status, actualKind, stage) => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    state.phase = stage;
    state.stages[stage].status = "awaiting_worker";
    await store.save(state);
    const expectedKind =
      stage === "baseline_evaluation" ? "evaluation" : "replay";
    const job = {
      error: status === "completed" ? null : "worker stopped",
      id: id(50),
      kind: actualKind,
      status,
    } as Parameters<typeof verifyCompletedJob>[0];

    await expect(
      verifyCompletedJob(job, expectedKind, state, store, stage),
    ).rejects.toThrow(`Expected completed ${expectedKind} job`);
    expect((await store.load())?.phase).toBe(stage);
    expect((await store.load())?.stages[stage].status).toBe("failed");
  });
});

describe("canonical machine events", () => {
  it("sorts exact-job handoffs and emits no credential fields", () => {
    const event = createWorkerHandoffEvent({
      evidenceSetId: id(2),
      jobs: [
        { agent_version_id: id(8), job_id: id(9), job_kind: "replay" },
        { agent_version_id: id(8), job_id: id(7), job_kind: "replay" },
      ],
      phase: "experiment_runs",
    });

    expect(event).toEqual({
      event: "kitaru.worker_handoff",
      schema_version: 1,
      evidence_set_id: id(2),
      phase: "experiment_runs",
      manifest_relative_path: ".state/workflow.json",
      jobs: [
        { agent_version_id: id(8), job_id: id(7), job_kind: "replay" },
        { agent_version_id: id(8), job_id: id(9), job_kind: "replay" },
      ],
    });
    expect(JSON.stringify(event)).not.toMatch(/token|credential|api_key/i);
  });

  it("reports only accepted completion counts", () => {
    expect(
      createCompletedEvent(id(2), {
        baseline_failures: 2,
        baseline_passes: 8,
        baseline_sessions: 10,
        control_sessions: 3,
        experiment_runs: 2,
        replay_passes: 5,
        replays: 5,
        target_sessions: 2,
      }),
    ).toMatchObject({
      event: "kitaru.workflow_completed",
      schema_version: 1,
      evidence_set_id: id(2),
    });
  });
});

describe("issue 767 request contract", () => {
  it("uses the exact reviewed target, control, evaluator, and replay inputs", () => {
    expect(BASELINE_FAILURE_TICKETS).toEqual(["ticket-004", "ticket-007"]);
    expect(TARGET_TICKETS).toEqual(["ticket-004", "ticket-007"]);
    expect(CONTROL_TICKETS).toEqual(["ticket-001", "ticket-009", "ticket-010"]);
    expect(TOOLS).toEqual([
      "lookup_order",
      "get_return_policy",
      "check_shipping",
      "issue_refund",
      "create_replacement",
      "escalate_to_human",
    ]);
    expect(workflowRequests.createEvaluator("returns-policy", 7)).toEqual({
      evaluators: [{ evaluator: "returns-policy", version: 7 }],
    });
    expect(workflowRequests.createExperimentRun(id(3), id(4))).toEqual({
      agent_version_id: id(3),
      cohort_version_id: id(4),
      evaluate_baselines: false,
    });
    expect(workflowRequests.toolPolicy).toEqual({
      default: { type: "passthrough" },
    });
  });
});
