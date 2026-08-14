import { describe, expect, it, vi } from "vitest";

import {
  buildDedicatedWorkerInvocation,
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

  it("lets the Python CLI resolve its stored login without exporting a token", async () => {
    const spawn = vi.fn((_command, _args, options) => {
      expect(options.env).toMatchObject({
        HOME: "/Users/test",
        KITARU_CONFIG_DIR: "/tmp/kitaru-config",
        PATH: "/usr/local/bin",
      });
      expect(options.env).not.toHaveProperty("KITARU_API_KEY");
      expect(options.env).not.toHaveProperty("KITARU_API_TOKEN");
      return Promise.resolve({ code: 0, signal: null });
    });

    await runDedicatedWorker(
      { jobId: JOB_ID, stateDir: "/tmp/kitaru-mastra-run" },
      {
        environment: {
          HOME: "/Users/test",
          KITARU_CONFIG_DIR: "/tmp/kitaru-config",
          PATH: "/usr/local/bin",
          UNRELATED_SECRET: "must-not-cross",
        },
        spawn,
      },
    );

    expect(spawn).toHaveBeenCalledTimes(1);
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
