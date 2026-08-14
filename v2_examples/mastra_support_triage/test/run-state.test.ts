import { chmod, mkdtemp, readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  AmbiguousOperationError,
  operationFingerprint,
  RunManifestStore,
} from "../src/run-state.js";

const RUN_ID = "018f0000-0000-7000-8000-000000000001";
const OWNER_ID = "018f0000-0000-7000-8000-000000000002";

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
