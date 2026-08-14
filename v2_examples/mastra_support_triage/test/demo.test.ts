import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { KitaruClient, ReplayResponse } from "@zenml-io/kitaru";
import { describe, expect, it, vi } from "vitest";

import { parseArguments, runDemo } from "../src/demo.js";
import { RunManifestStore } from "../src/run-state.js";

const RUN_ID = "018f0000-0000-7000-8000-000000000030";
const OWNER_ID = "018f0000-0000-7000-8000-000000000031";
const INITIAL_SESSION_ID = "018f0000-0000-7000-8000-000000000032";
const REPLAY_ID = "018f0000-0000-7000-8000-000000000033";
const REPLAY_JOB_ID = "018f0000-0000-7000-8000-000000000034";
const RESULT_SESSION_ID = "018f0000-0000-7000-8000-000000000035";

describe("runDemo recovery", () => {
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
});
