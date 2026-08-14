import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  AmbiguousOperationError,
  operationFingerprint,
  RunManifestStore,
} from "../src/run-state.js";

const RUN_ID = "018f0000-0000-7000-8000-000000000001";
const OWNER_ID = "018f0000-0000-7000-8000-000000000002";
const OLD_JOB_ID = "018f0000-0000-7000-8000-000000000007";
const NEW_JOB_ID = "018f0000-0000-7000-8000-000000000008";

describe("RunManifestStore", () => {
  it("writes owner-only state in one invocation-specific directory", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    await chmod(root, 0o755);
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });

    const directory = await stat(store.stateDir);
    const file = await stat(store.path);
    const raw = JSON.parse(await readFile(store.path, "utf8")) as {
      owner_id: string;
      run_id: string;
    };

    expect(directory.mode & 0o777).toBe(0o700);
    expect(file.mode & 0o777).toBe(0o600);
    expect(raw).toMatchObject({ owner_id: OWNER_ID, run_id: RUN_ID });
    expect(store.stateDir).toBe(join(root, RUN_ID));
  });

  it("records an ambiguous create and refuses to issue it again", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const createRemote = vi
      .fn<() => Promise<{ id: string }>>()
      .mockRejectedValue(new Error("response lost"));
    const fingerprint = operationFingerprint({ name: "agent" });

    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow(AmbiguousOperationError);
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow("--adopt create_agent=UUID or --retry create_agent");

    expect(createRemote).toHaveBeenCalledTimes(1);
    expect((await store.read()).operations).toMatchObject([
      {
        fingerprint,
        kind: "create_agent",
        state: "ambiguous",
      },
    ]);
  });

  it("commits the exact returned identifiers before later work can run", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });

    await store.createRemote(
      "create_initial_job",
      operationFingerprint({ name: "initial-job" }),
      async () => ({ id: "018f0000-0000-7000-8000-000000000003" }),
      {
        commit(manifest, remote) {
          manifest.resources.initial_job_id = remote.id;
        },
      },
    );

    const manifest = await store.read();
    expect(manifest.resources.initial_job_id).toBe(
      "018f0000-0000-7000-8000-000000000003",
    );
    expect(manifest.operations.at(-1)).toMatchObject({
      remote_ids: ["018f0000-0000-7000-8000-000000000003"],
      state: "committed",
    });
  });

  it("rejects a concurrent resume before its action can issue remote calls", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    let releaseFirst!: () => void;
    const firstAction = store.withRunLock(
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
    );
    await vi.waitFor(() => expect(releaseFirst).toBeTypeOf("function"));
    const remoteCall = vi.fn();

    await expect(store.withRunLock(async () => remoteCall())).rejects.toThrow(
      "already active",
    );
    expect(remoteCall).not.toHaveBeenCalled();

    releaseFirst();
    await firstAction;
    await expect(store.withRunLock(async () => "released")).resolves.toBe(
      "released",
    );
    await expect(
      store.withRunLock(async () => {
        throw new Error("workflow failed");
      }),
    ).rejects.toThrow("workflow failed");
    await expect(
      store.withRunLock(async () => "released after failure"),
    ).resolves.toBe("released after failure");
  });

  it("atomically reclaims a lock owned by a dead local process", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const lockDirectory = join(store.stateDir, "run.lock");
    const ownerPath = join(lockDirectory, "owner.json");
    await mkdir(lockDirectory, { mode: 0o700 });
    await writeFile(
      ownerPath,
      `${JSON.stringify({
        hostname: hostname(),
        pid: 99_999_999,
        started_at: "2026-08-14T10:00:00.000Z",
        token: "stale-token",
      })}\n`,
      { mode: 0o600 },
    );

    const lock = await store.acquireRunLock();
    const owner = JSON.parse(await readFile(ownerPath, "utf8")) as {
      pid: number;
      token: string;
    };
    expect(owner.pid).toBe(process.pid);
    expect(owner.token).not.toBe("stale-token");
    expect((await stat(lockDirectory)).mode & 0o777).toBe(0o700);
    expect((await stat(ownerPath)).mode & 0o777).toBe(0o600);
    await lock.release();
  });

  it("does not release a lock whose ownership token changed", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const lockDirectory = join(store.stateDir, "run.lock");
    const ownerPath = join(lockDirectory, "owner.json");
    const lock = await store.acquireRunLock();
    await writeFile(
      ownerPath,
      `${JSON.stringify({
        hostname: hostname(),
        pid: process.pid,
        started_at: new Date().toISOString(),
        token: "replacement-owner",
      })}\n`,
      { mode: 0o600 },
    );

    await expect(lock.release()).rejects.toThrow("ownership changed");
    await expect(stat(lockDirectory)).resolves.toBeDefined();
    await rm(lockDirectory, { recursive: true });
  });

  it("journals an explicit committed-job replacement before creating it", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "initial-job" });
    await store.createRemote(
      "create_initial_job",
      fingerprint,
      async () => ({ id: OLD_JOB_ID }),
      {
        commit(manifest, remote) {
          manifest.resources.initial_job_id = remote.id;
        },
      },
    );

    await store.authorizeReplacement("create_initial_job", fingerprint, [
      OLD_JOB_ID,
    ]);
    const save = store.save.bind(store);
    const saveSpy = vi
      .spyOn(store, "save")
      .mockImplementationOnce(async (value) => {
        await save(value);
        throw new Error("process interrupted after replacement journal");
      });
    const createReplacement = vi.fn(async () => {
      expect((await store.read()).operations.at(-1)).toMatchObject({
        fingerprint,
        kind: "create_initial_job",
        state: "planned",
      });
      return { id: NEW_JOB_ID };
    });
    await expect(
      store.createRemote("create_initial_job", fingerprint, createReplacement, {
        retry: true,
      }),
    ).rejects.toThrow("process interrupted after replacement journal");
    saveSpy.mockRestore();
    expect(createReplacement).not.toHaveBeenCalled();

    await store.authorizeReplacement("create_initial_job", fingerprint, [
      OLD_JOB_ID,
    ]);
    await store.createRemote(
      "create_initial_job",
      fingerprint,
      createReplacement,
      {
        commit(manifest, remote) {
          manifest.resources.initial_job_id = remote.id;
        },
        retry: true,
      },
    );

    expect(await store.read()).toMatchObject({
      operations: [
        { remote_ids: [OLD_JOB_ID], state: "retried" },
        { remote_ids: [], state: "retried" },
        { remote_ids: [NEW_JOB_ID], state: "committed" },
      ],
      resources: { initial_job_id: NEW_JOB_ID },
    });
  });

  it("authorizes replacement only for the exact committed operation", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "replay" });
    await store.createRemote("create_replay", fingerprint, async () => ({
      id: OLD_JOB_ID,
      job_id: NEW_JOB_ID,
    }));

    const mismatches = [
      ["create_initial_job", fingerprint, [OLD_JOB_ID, NEW_JOB_ID]],
      [
        "create_replay",
        operationFingerprint({ name: "changed" }),
        [OLD_JOB_ID, NEW_JOB_ID],
      ],
      ["create_replay", fingerprint, [OLD_JOB_ID]],
      ["create_replay", fingerprint, [OLD_JOB_ID, OWNER_ID]],
      ["create_replay", fingerprint, [NEW_JOB_ID, OLD_JOB_ID]],
    ] as const;
    for (const [kind, candidateFingerprint, remoteIds] of mismatches) {
      await expect(
        store.authorizeReplacement(kind, candidateFingerprint, remoteIds),
      ).rejects.toThrow("No exact committed");
    }

    expect((await store.read()).operations.at(-1)?.state).toBe("committed");
    await store.authorizeReplacement("create_replay", fingerprint, [
      OLD_JOB_ID,
      NEW_JOB_ID,
    ]);
    expect((await store.read()).operations.at(-1)).toMatchObject({
      fingerprint,
      kind: "create_replay",
      remote_ids: [OLD_JOB_ID, NEW_JOB_ID],
      state: "retried",
    });
  });

  it("adopts an exact validated remote object without issuing the create again", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "agent" });
    const createRemote = vi.fn().mockRejectedValue(new Error("response lost"));
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow(AmbiguousOperationError);
    const adoptedId = "018f0000-0000-7000-8000-000000000004";
    const adopt = vi
      .fn()
      .mockResolvedValue({ id: adoptedId, owner_id: OWNER_ID });
    const validateAdopted = vi.fn();

    await store.createRemote("create_agent", fingerprint, createRemote, {
      adopt,
      adoptionId: adoptedId,
      commit(manifest, remote) {
        manifest.resources.agent_id = remote.id;
      },
      validateAdopted,
    });

    expect(createRemote).toHaveBeenCalledTimes(1);
    expect(adopt).toHaveBeenCalledWith(adoptedId);
    expect(validateAdopted).toHaveBeenCalledWith({
      id: adoptedId,
      owner_id: OWNER_ID,
    });
    expect(await store.read()).toMatchObject({
      operations: [{ remote_ids: [adoptedId], state: "committed" }],
      resources: { agent_id: adoptedId },
    });
  });

  it("retries only after an explicit exact operation request", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "agent" });
    const retriedId = "018f0000-0000-7000-8000-000000000005";
    const createRemote = vi
      .fn<() => Promise<{ id: string }>>()
      .mockRejectedValueOnce(new Error("response lost"))
      .mockResolvedValueOnce({ id: retriedId });
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow(AmbiguousOperationError);

    await store.createRemote("create_agent", fingerprint, createRemote, {
      retry: true,
    });

    expect(createRemote).toHaveBeenCalledTimes(2);
    expect((await store.read()).operations).toMatchObject([
      { fingerprint, kind: "create_agent", state: "retried" },
      {
        fingerprint,
        kind: "create_agent",
        remote_ids: [retriedId],
        state: "committed",
      },
    ]);
  });

  it("resumes an authorized retry after interruption before replacement journaling", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "agent" });
    const retriedId = "018f0000-0000-7000-8000-000000000006";
    const createRemote = vi
      .fn<() => Promise<{ id: string }>>()
      .mockRejectedValueOnce(new Error("response lost"))
      .mockResolvedValueOnce({ id: retriedId });
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow(AmbiguousOperationError);

    const save = store.save.bind(store);
    const saveSpy = vi
      .spyOn(store, "save")
      .mockImplementationOnce(async (value) => {
        await save(value);
        throw new Error(
          "process interrupted after durable retry authorization",
        );
      });
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote, {
        retry: true,
      }),
    ).rejects.toThrow("process interrupted");
    saveSpy.mockRestore();

    expect((await store.read()).operations.at(-1)?.state).toBe("retried");
    await expect(
      store.createRemote(
        "create_evaluator",
        operationFingerprint({ name: "evaluator" }),
        vi.fn(),
        { retry: true },
      ),
    ).rejects.toThrow("No ambiguous or authorized");
    await store.createRemote("create_agent", fingerprint, createRemote, {
      retry: true,
    });

    expect(createRemote).toHaveBeenCalledTimes(2);
    expect((await store.read()).operations).toMatchObject([
      { state: "retried" },
      { remote_ids: [retriedId], state: "committed" },
    ]);
  });

  it("rejects recovery for a different operation or fingerprint", async () => {
    const root = await mkdtemp(join(tmpdir(), "kitaru-mastra-state-"));
    const store = await RunManifestStore.create({
      ownerId: OWNER_ID,
      rootDir: root,
      runId: RUN_ID,
      serverUrl: "https://kitaru.example.test",
    });
    const fingerprint = operationFingerprint({ name: "agent" });
    const createRemote = vi.fn().mockRejectedValue(new Error("response lost"));
    await expect(
      store.createRemote("create_agent", fingerprint, createRemote),
    ).rejects.toThrow(AmbiguousOperationError);

    await expect(
      store.createRemote(
        "create_evaluator",
        operationFingerprint({ name: "evaluator" }),
        vi.fn(),
        { retry: true },
      ),
    ).rejects.toThrow("does not match");
    await expect(
      store.createRemote(
        "create_agent",
        operationFingerprint({ name: "changed" }),
        vi.fn(),
        { retry: true },
      ),
    ).rejects.toThrow("does not match");
  });
});
