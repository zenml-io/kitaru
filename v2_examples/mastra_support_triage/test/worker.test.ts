import { describe, expect, it, vi } from "vitest";

import {
  buildDedicatedWorkerInvocation,
  preflightDedicatedWorker,
  runDedicatedWorker,
} from "../src/worker.js";

const JOB_ID = "018f0000-0000-7000-8000-000000000010";

describe("dedicated worker invocation", () => {
  it("always scopes claims to the exact durable job id", () => {
    const invocation = buildDedicatedWorkerInvocation({
      executable: "kitaru",
      jobId: JOB_ID,
      stateDir: "/tmp/kitaru-mastra-run",
    });

    expect(invocation.command).toBe("kitaru");
    expect(invocation.args).toEqual([
      "worker",
      "start",
      "--job-id",
      JOB_ID,
      "--name",
      `mastra-demo-${JOB_ID}`,
      "--concurrency",
      "1",
      "--claim-batch-size",
      "1",
      "--poll-interval",
      "0.05",
      "--timeout",
      "180",
      "--blob-cache-root",
      "/tmp/kitaru-mastra-run/worker-blobs",
      "--payload-cache-root",
      "/tmp/kitaru-mastra-run/worker-payloads",
    ]);
  });

  it("passes only the credentials and runtime state needed by the trusted worker", async () => {
    const spawn = vi.fn((_command, _args, options) => {
      expect(options.env).toMatchObject({
        HOME: "/Users/test",
        KITARU_API_KEY: "KITKEY_test",
        KITARU_API_TOKEN: "access-token",
        KITARU_CONFIG_DIR: "/tmp/kitaru-config",
        PATH: "/usr/local/bin",
      });
      expect(options.env).not.toHaveProperty("UNRELATED_SECRET");
      return Promise.resolve({ code: 0, signal: null });
    });

    await runDedicatedWorker(
      { jobId: JOB_ID, stateDir: "/tmp/kitaru-mastra-run" },
      {
        environment: {
          HOME: "/Users/test",
          KITARU_API_KEY: "KITKEY_test",
          KITARU_API_TOKEN: "access-token",
          KITARU_CONFIG_DIR: "/tmp/kitaru-config",
          PATH: "/usr/local/bin",
          UNRELATED_SECRET: "must-not-cross",
        },
        spawn,
      },
    );

    expect(spawn).toHaveBeenCalledTimes(1);
  });

  it("checks the worker CLI authentication before the demo mutates remote state", async () => {
    const spawn = vi.fn().mockResolvedValue({ code: 3, signal: null });

    await expect(
      preflightDedicatedWorker(
        { apiUrl: "https://kitaru.example.test" },
        {
          environment: { KITARU_API_KEY: "invalid", PATH: "/usr/bin" },
          spawn,
        },
      ),
    ).rejects.toThrow(
      "Dedicated worker authentication preflight exited with code 3",
    );
    expect(spawn).toHaveBeenCalledWith(
      "kitaru",
      [
        "doctor",
        "--server",
        "https://kitaru.example.test",
        "--output",
        "json",
        "--machine",
        "--non-interactive",
        "--no-browser",
      ],
      expect.objectContaining({
        env: {
          KITARU_API_KEY: "invalid",
          PATH: "/usr/bin",
        },
      }),
    );
  });

  it("reports a non-zero worker exit without starting a second worker", async () => {
    const spawn = vi.fn().mockResolvedValue({ code: 17, signal: null });

    await expect(
      runDedicatedWorker(
        { jobId: JOB_ID, stateDir: "/tmp/kitaru-mastra-run" },
        { spawn },
      ),
    ).rejects.toThrow(`Dedicated worker for job ${JOB_ID} exited with code 17`);
    expect(spawn).toHaveBeenCalledTimes(1);
  });
});
