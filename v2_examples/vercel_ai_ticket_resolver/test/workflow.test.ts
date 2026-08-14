import { mkdtemp, readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type {
  AgentVersionResponse,
  CohortVersionResponse,
  EvaluatorVersionResponse,
  KitaruClient,
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
  ensureExperiment,
  findInvestigationsByName,
  getCohortVersionForAdoption,
  getEvaluatorVersionForAdoption,
  parseWorkflowArguments,
  runJournaledMutation,
  runWorkflow,
  TARGET_TICKETS,
  TOOLS,
  validateAdoptedAnnotation,
  verifyCompletedJob,
  workflowRequests,
} from "../src/workflow-runner.js";

const id = (index: number) =>
  `018f0000-0000-7000-8000-${String(index).padStart(12, "0")}`;
const exampleDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const sourceMaterial = {
  evaluatorSource: "def evaluate():\n    return True\n",
  hashes: {
    baseline_instructions_sha256: "a".repeat(64),
    evaluator_sha256: "b".repeat(64),
    fixtures_sha256: "c".repeat(64),
    strict_instructions_sha256: "d".repeat(64),
  },
};

function manifest(): WorkflowManifest {
  return createWorkflowManifest({
    accountId: id(1),
    apiUrl: "https://kitaru.example",
    authScheme: "control_plane",
    evidenceSetId: id(2),
    serverVersion: "0.22.0",
    sourceHashes: sourceMaterial.hashes,
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

  it.each([
    ["api_url", "https://other.example"],
    ["account_id", id(90)],
    ["auth_scheme", "local"],
    ["version", "0.23.0"],
  ] as const)("rejects a changed server %s before any remote mutation", async (field, value) => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    state.server = { ...state.server, [field]: value };
    await store.save(state);
    const createAgent = vi.fn();
    const client = {
      accounts: { getCurrent: vi.fn(async () => ({ id: id(1) })) },
      agents: { create: createAgent },
      info: {
        get: vi.fn(async () => ({
          auth_scheme: "control_plane",
          version: "0.22.0",
        })),
      },
    } as unknown as KitaruClient;

    await expect(
      runWorkflow(
        parseWorkflowArguments(["--state-dir", directory]),
        { KITARU_API_URL: "https://kitaru.example" },
        {
          createClient: async () => client,
          readSourceMaterial: async () => sourceMaterial,
        },
      ),
    ).rejects.toThrow("belongs to another server or account");
    expect(createAgent).not.toHaveBeenCalled();
  });

  it("rejects a changed stored agent version before resuming work", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    state.ids.agent_id = id(3);
    state.ids.agent_versions.baseline = id(4);
    await store.save(state);
    const storedVersion: AgentVersionResponse = {
      agent_id: state.ids.agent_id,
      capabilities: {
        mcp_servers: [],
        skills: [],
        tools: [...TOOLS],
      },
      created: "2026-08-14T00:00:00Z",
      description: "Deterministic baseline returns policy.",
      display_version: "baseline-v1",
      id: state.ids.agent_versions.baseline,
      owner_id: id(1),
      run_spec: {
        command: "node changed-entrypoint.js",
        env: {
          KITARU_AGENT_ID: state.ids.agent_id,
          RETURNS_MODEL_PROVIDER: "deterministic",
          RETURNS_POLICY_MODE: "baseline",
        },
        secret_ids: [],
        timeout_seconds: 180,
        working_dir: exampleDirectory,
      },
      updated: "2026-08-14T00:00:00Z",
      version: 1,
    };
    const client = {
      accounts: { getCurrent: vi.fn(async () => ({ id: id(1) })) },
      agents: {
        get: vi.fn(async () => ({ id: state.ids.agent_id })),
        getVersion: vi.fn(async () => storedVersion),
      },
      info: {
        get: vi.fn(async () => ({
          auth_scheme: "control_plane",
          version: "0.22.0",
        })),
      },
    } as unknown as KitaruClient;

    await expect(
      runWorkflow(
        parseWorkflowArguments(["--state-dir", directory]),
        { KITARU_API_URL: "https://kitaru.example" },
        {
          createClient: async () => client,
          readSourceMaterial: async () => sourceMaterial,
        },
      ),
    ).rejects.toThrow("Agent version does not match the workflow definition");
    expect(client.agents.getVersion).toHaveBeenCalledWith(
      state.ids.agent_versions.baseline,
    );
  });

  it("rejects a changed stored experiment before starting runs", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    state.ids.agent_id = id(3);
    state.ids.experiment_id = id(4);
    const experiment = {
      agent_id: state.ids.agent_id,
      created: "2026-08-14T00:00:00Z",
      description: null,
      evaluators: [{ evaluator: "policy", version: 1 }],
      id: state.ids.experiment_id,
      name: `improve-returns-policy-${state.evidence_set_id.replaceAll("-", "").slice(0, 12)}`,
      override: null,
      owner_id: id(1),
      tool_policy: { default: { type: "deny" }, tools: {} },
      updated: "2026-08-14T00:00:00Z",
    } as const;
    const client = {
      experiments: { get: vi.fn(async () => experiment) },
    } as unknown as KitaruClient;

    await expect(
      ensureExperiment(
        client,
        state,
        store,
        parseWorkflowArguments(["--state-dir", directory]),
        { name: "policy" },
        { version: 1 },
      ),
    ).rejects.toThrow("Experiment does not match the workflow definition");
    expect(client.experiments.get).toHaveBeenCalledWith(
      state.ids.experiment_id,
    );
  });

  it("holds one filesystem lock from before state work through workflow exit", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    let releaseSourceMaterial:
      | ((value: typeof sourceMaterial) => void)
      | undefined;
    let sourceReadStarted: (() => void) | undefined;
    const started = new Promise<void>((resolveStarted) => {
      sourceReadStarted = resolveStarted;
    });
    const blockedSourceMaterial = new Promise<typeof sourceMaterial>(
      (resolve) => {
        releaseSourceMaterial = resolve;
      },
    );
    const readSourceMaterial = vi
      .fn<() => Promise<typeof sourceMaterial>>()
      .mockImplementationOnce(async () => {
        sourceReadStarted?.();
        return blockedSourceMaterial;
      })
      .mockResolvedValue(sourceMaterial);
    const remoteCalls = vi.fn(async () => {
      throw new Error("stop after lock verification");
    });
    const client = {
      accounts: { getCurrent: remoteCalls },
      info: { get: remoteCalls },
    } as unknown as KitaruClient;
    const dependencies = {
      createClient: vi.fn(async () => client),
      readSourceMaterial,
    };
    const args = parseWorkflowArguments(["--state-dir", directory]);
    const environment = { KITARU_API_URL: "https://kitaru.example" };

    const first = runWorkflow(args, environment, dependencies);
    await started;
    await expect(runWorkflow(args, environment, dependencies)).rejects.toThrow(
      "already running",
    );
    expect(dependencies.createClient).not.toHaveBeenCalled();
    expect(remoteCalls).not.toHaveBeenCalled();
    expect(readSourceMaterial).toHaveBeenCalledOnce();

    releaseSourceMaterial?.(sourceMaterial);
    await expect(first).rejects.toThrow("stop after lock verification");
    expect(dependencies.createClient).toHaveBeenCalledOnce();
    expect(remoteCalls).toHaveBeenCalledTimes(2);

    await expect(runWorkflow(args, environment, dependencies)).rejects.toThrow(
      "stop after lock verification",
    );
    expect(dependencies.createClient).toHaveBeenCalledTimes(2);
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

  it("persists the mutation journal before calling the remote create", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const remote = { id: id(60), name: "journal-first" };
    const create = vi.fn(async () => {
      expect((await store.load())?.pending_operation).toMatchObject({
        key: "agent",
        status: "submitted",
      });
      return remote;
    });

    await runJournaledMutation(
      {
        commit: (current, value) => {
          current.ids.agent_id = value.id;
        },
        create,
        fingerprintInput: { name: remote.name },
        key: "agent",
        kind: "agent",
        manifest: state,
        stage: "baseline",
        store,
        validate: () => {},
      },
      { adoptions: {}, retries: new Set() },
    );

    expect(create).toHaveBeenCalledOnce();
  });

  it("reconciles one exact candidate before honoring retry", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const orphan = { id: id(61), name: "retry-precedence" };
    const create = vi
      .fn<() => Promise<typeof orphan>>()
      .mockResolvedValueOnce(orphan);
    const reconcile = vi.fn(async () => [orphan]);
    const input = {
      commit: (current: WorkflowManifest, value: typeof orphan) => {
        current.ids.agent_id = value.id;
      },
      create,
      fingerprintInput: { name: orphan.name },
      key: "agent",
      kind: "agent",
      manifest: state,
      reconcile,
      stage: "baseline" as const,
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
      runJournaledMutation(input, {
        adoptions: {},
        retries: new Set(["agent"]),
      }),
    ).resolves.toEqual(orphan);
    expect(reconcile).toHaveBeenCalledOnce();
    expect(create).toHaveBeenCalledOnce();
    expect((await store.load())?.ids.agent_id).toBe(orphan.id);
  });

  it("keeps multiple reconciliation candidates ambiguous despite retry", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const first = { id: id(65), name: "ambiguous-retry" };
    const second = { id: id(66), name: "ambiguous-retry" };
    const create = vi.fn(async () => first);
    const input = {
      commit: (current: WorkflowManifest, value: typeof first) => {
        current.ids.agent_id = value.id;
      },
      create,
      fingerprintInput: { name: first.name },
      key: "agent",
      kind: "agent",
      manifest: state,
      reconcile: async () => [first, second],
      stage: "baseline" as const,
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
      runJournaledMutation(input, {
        adoptions: {},
        retries: new Set(["agent"]),
      }),
    ).rejects.toThrow("ambiguous");
    expect(create).toHaveBeenCalledOnce();
    expect((await store.load())?.pending_operation).toMatchObject({
      status: "ambiguous",
    });
  });

  it("resumes a retry interrupted after durable authorization", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    await store.save(state);
    const orphan = { id: id(63), name: "durable-retry" };
    const replacement = { id: id(64), name: "durable-retry" };
    const create = vi
      .fn<() => Promise<typeof orphan>>()
      .mockResolvedValueOnce(orphan)
      .mockResolvedValueOnce(replacement);
    const reconcile = vi.fn(async () => []);
    const input = {
      commit: (current: WorkflowManifest, value: typeof orphan) => {
        current.ids.agent_id = value.id;
      },
      create,
      fingerprintInput: { name: orphan.name },
      key: "agent",
      kind: "agent",
      manifest: state,
      reconcile,
      stage: "baseline" as const,
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

    const save = store.save.bind(store);
    const saveSpy = vi
      .spyOn(store, "save")
      .mockImplementation(async (value) => {
        await save(value);
        if (
          (value.pending_operation as { status?: string } | null)?.status ===
          "retry_authorized"
        ) {
          throw new Error("interrupted after retry authorization");
        }
      });
    await expect(
      runJournaledMutation(input, {
        adoptions: {},
        retries: new Set(["agent"]),
      }),
    ).rejects.toThrow("interrupted after retry authorization");
    saveSpy.mockRestore();
    expect((await store.load())?.pending_operation).toMatchObject({
      key: "agent",
      status: "retry_authorized",
    });

    const resumed = (await store.load()) as WorkflowManifest;
    await expect(
      runJournaledMutation(
        { ...input, manifest: resumed },
        { adoptions: {}, retries: new Set() },
      ),
    ).resolves.toEqual(replacement);
    expect(reconcile).toHaveBeenCalledOnce();
    expect(create).toHaveBeenCalledTimes(2);
  });

  it("requires a new explicit choice after a retry was submitted", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kitaru-workflow-"));
    const store = new WorkflowManifestStore(directory);
    const state = manifest();
    const orphan = { id: id(67), name: "submitted-retry" };
    const replacement = { id: id(68), name: "submitted-retry" };
    const create = vi
      .fn<() => Promise<typeof orphan>>()
      .mockResolvedValueOnce(orphan)
      .mockResolvedValueOnce(replacement);
    const reconcile = vi.fn(async () => []);
    const createInput = (current: WorkflowManifest) => ({
      commit: (current: WorkflowManifest, value: typeof replacement) => {
        current.ids.agent_id = value.id;
      },
      create,
      fingerprintInput: { name: replacement.name },
      key: "agent",
      kind: "agent",
      manifest: current,
      reconcile,
      stage: "baseline" as const,
      store,
      validate: () => {},
    });

    await expect(
      runJournaledMutation(
        {
          ...createInput(state),
          afterRemoteCommit: async () => {
            throw new Error("lost response");
          },
        },
        { adoptions: {}, retries: new Set() },
      ),
    ).rejects.toThrow("lost response");

    const save = store.save.bind(store);
    const saveSpy = vi
      .spyOn(store, "save")
      .mockImplementation(async (value) => {
        await save(value);
        if (value.pending_operation?.status === "retry_submitted") {
          throw new Error("interrupted after retry submission");
        }
      });
    await expect(
      runJournaledMutation(createInput(state), {
        adoptions: {},
        retries: new Set(["agent"]),
      }),
    ).rejects.toThrow("interrupted after retry submission");
    saveSpy.mockRestore();
    const retrySubmitted = (await store.load()) as WorkflowManifest;
    expect(retrySubmitted.pending_operation?.status).toBe("retry_submitted");
    const explicitRetry = structuredClone(retrySubmitted);
    reconcile.mockClear();

    await expect(
      runJournaledMutation(createInput(retrySubmitted), {
        adoptions: {},
        retries: new Set(),
      }),
    ).rejects.toThrow("ambiguous");
    expect(reconcile).not.toHaveBeenCalled();
    expect(create).toHaveBeenCalledOnce();

    await store.save(explicitRetry);
    await expect(
      runJournaledMutation(createInput(explicitRetry), {
        adoptions: {},
        retries: new Set(["agent"]),
      }),
    ).resolves.toEqual(replacement);
    expect(reconcile).not.toHaveBeenCalled();
    expect(create).toHaveBeenCalledTimes(2);
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

describe("review annotation recovery", () => {
  it("accepts only the exact investigation question being adopted", () => {
    const expected = {
      investigationSessionId: id(70),
      questionKey: "outcome",
      selector: { node_id: id(73), path: "/outputs" },
      sessionId: id(71),
      value: { judgment: "problematic" },
    };
    const annotation = {
      investigation_session_id: expected.investigationSessionId,
      question_key: expected.questionKey,
      selector: { ...expected.selector, span: null },
      session_id: expected.sessionId,
      value: expected.value,
    };

    expect(() => validateAdoptedAnnotation(annotation, expected)).not.toThrow();
    expect(() =>
      validateAdoptedAnnotation(
        { ...annotation, question_key: "expected" },
        expected,
      ),
    ).toThrow("does not match the investigation question");
    expect(() =>
      validateAdoptedAnnotation(
        { ...annotation, selector: { node_id: id(74), path: "/outputs" } },
        expected,
      ),
    ).toThrow("does not match the answer selector");
    expect(() =>
      validateAdoptedAnnotation(
        { ...annotation, value: { judgment: "acceptable" } },
        expected,
      ),
    ).toThrow("does not match the answer value");
    expect(() =>
      validateAdoptedAnnotation(
        { ...annotation, investigation_session_id: id(72) },
        expected,
      ),
    ).toThrow("does not match the investigation question");
  });
});

describe("canonical machine events", () => {
  it("sorts exact-job handoffs and emits no credential fields", () => {
    const stateDirectory = join(process.cwd(), "custom-workflow-state");
    const event = createWorkerHandoffEvent({
      evidenceSetId: id(2),
      jobs: [
        { agent_version_id: id(8), job_id: id(9), job_kind: "replay" },
        { agent_version_id: id(8), job_id: id(7), job_kind: "replay" },
      ],
      phase: "experiment_runs",
      stateDirectory,
    });

    expect(event).toEqual({
      event: "kitaru.worker_handoff",
      schema_version: 1,
      evidence_set_id: id(2),
      phase: "experiment_runs",
      manifest_relative_path: join("custom-workflow-state", "workflow.json"),
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

describe("canonical investigation recovery", () => {
  it("paginates a supported agent filter and matches the exact name locally", async () => {
    const requested: unknown[] = [];
    const client = {
      investigations: {
        iter: async function* (params: unknown) {
          requested.push(params);
          yield { agent_id: id(70), id: id(71), name: "another-run" };
          yield { agent_id: id(70), id: id(72), name: "exact-run" };
          yield { agent_id: id(70), id: id(73), name: "exact-run-suffix" };
        },
      },
    };

    await expect(
      findInvestigationsByName(client, id(70), "exact-run"),
    ).resolves.toEqual([
      expect.objectContaining({ id: id(72), name: "exact-run" }),
    ]);
    expect(requested).toEqual([
      {
        filter: { field: "agent_id", op: "eq", value: id(70) },
        size: 100,
      },
    ]);
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
