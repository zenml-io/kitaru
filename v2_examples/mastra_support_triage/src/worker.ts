import { spawn as spawnChild } from "node:child_process";
import { join } from "node:path";

export interface WorkerInvocationOptions {
  apiUrl?: string;
  executable?: string;
  jobId: string;
  stateDir: string;
}

export interface WorkerInvocation {
  args: string[];
  command: string;
}

export interface SpawnResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export type SpawnWorker = (
  command: string,
  args: readonly string[],
  options: {
    env: NodeJS.ProcessEnv;
    signal?: AbortSignal;
  },
) => Promise<SpawnResult>;

export interface RunDedicatedWorkerDependencies {
  environment?: NodeJS.ProcessEnv;
  signal?: AbortSignal;
  spawn?: SpawnWorker;
}

const PASSTHROUGH_ENVIRONMENT = [
  "HOME",
  "KITARU_CONFIG_DIR",
  "LANG",
  "LC_ALL",
  "OPENAI_API_KEY",
  "PATH",
  "SHELL",
  "TMPDIR",
  "USER",
  "XDG_CONFIG_HOME",
] as const;

function workerEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return Object.fromEntries(
    PASSTHROUGH_ENVIRONMENT.flatMap((name) => {
      const value = environment[name];
      return value === undefined ? [] : [[name, value]];
    }),
  );
}

const spawnWorker: SpawnWorker = (command, args, options) =>
  new Promise((resolve, reject) => {
    const child = spawnChild(command, args, {
      env: options.env,
      signal: options.signal,
      stdio: "inherit",
    });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });

export function buildDedicatedWorkerInvocation(
  options: WorkerInvocationOptions,
): WorkerInvocation {
  return {
    args: [
      "worker",
      "start",
      "--job-id",
      options.jobId,
      "--name",
      `mastra-demo-${options.jobId}`,
      "--concurrency",
      "1",
      "--claim-batch-size",
      "1",
      "--poll-interval",
      "0.05",
      "--timeout",
      "180",
      "--blob-cache-root",
      join(options.stateDir, "worker-blobs"),
      "--payload-cache-root",
      join(options.stateDir, "worker-payloads"),
    ],
    command: options.executable ?? "kitaru",
  };
}

export async function runDedicatedWorker(
  options: WorkerInvocationOptions,
  dependencies: RunDedicatedWorkerDependencies = {},
): Promise<void> {
  const invocation = buildDedicatedWorkerInvocation(options);
  const environment = workerEnvironment(
    dependencies.environment ?? process.env,
  );
  if (options.apiUrl !== undefined) {
    environment.KITARU_API_URL = options.apiUrl;
  }
  const result = await (dependencies.spawn ?? spawnWorker)(
    invocation.command,
    invocation.args,
    {
      env: environment,
      signal: dependencies.signal,
    },
  );
  if (result.code !== 0) {
    const outcome =
      result.code === null
        ? `was terminated by ${result.signal ?? "an unknown signal"}`
        : `exited with code ${result.code}`;
    throw new Error(`Dedicated worker for job ${options.jobId} ${outcome}`);
  }
}
