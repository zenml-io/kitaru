import { spawn as spawnChild } from "node:child_process";
import { join } from "node:path";

export interface WorkerInvocationOptions {
  apiUrl?: string;
  executable?: string;
  jobId: string;
  stateDir: string;
}

export interface WorkerPreflightOptions {
  apiUrl?: string;
  executable?: string;
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
    stdio?: "ignore" | "inherit";
  },
) => Promise<SpawnResult>;

export interface RunDedicatedWorkerDependencies {
  environment?: NodeJS.ProcessEnv;
  signal?: AbortSignal;
  spawn?: SpawnWorker;
}

const PASSTHROUGH_ENVIRONMENT = [
  "HOME",
  "KITARU_API_KEY",
  "KITARU_API_TOKEN",
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
      stdio: options.stdio ?? "inherit",
    });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });

function getWorkerExitOutcome(result: SpawnResult): string {
  return result.code === null
    ? `was terminated by ${result.signal ?? "an unknown signal"}`
    : `exited with code ${result.code}`;
}

export function buildDedicatedWorkerInvocation(
  options: WorkerInvocationOptions,
): WorkerInvocation {
  const serverArgs =
    options.apiUrl === undefined ? [] : ["--server", options.apiUrl];
  return {
    args: [
      "worker",
      "start",
      ...serverArgs,
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

export async function preflightDedicatedWorker(
  options: WorkerPreflightOptions = {},
  dependencies: RunDedicatedWorkerDependencies = {},
): Promise<void> {
  const spawn = dependencies.spawn ?? spawnWorker;
  const executable = options.executable ?? "kitaru";
  const spawnOptions = {
    env: workerEnvironment(dependencies.environment ?? process.env),
    signal: dependencies.signal,
    stdio: "ignore" as const,
  };
  const cliResult = await spawn(
    executable,
    ["worker", "start", "--help"],
    spawnOptions,
  );
  if (cliResult.code !== 0) {
    throw new Error(
      `Dedicated worker CLI preflight ${getWorkerExitOutcome(cliResult)}`,
    );
  }
  const serverArgs =
    options.apiUrl === undefined ? [] : ["--server", options.apiUrl];
  const serverResult = await spawn(
    executable,
    [
      "agent",
      "list",
      ...serverArgs,
      "--size",
      "1",
      "--output",
      "json",
      "--machine",
      "--non-interactive",
      "--no-browser",
    ],
    spawnOptions,
  );
  if (serverResult.code !== 0) {
    throw new Error(
      `Dedicated worker server preflight ${getWorkerExitOutcome(serverResult)}`,
    );
  }
}

export async function runDedicatedWorker(
  options: WorkerInvocationOptions,
  dependencies: RunDedicatedWorkerDependencies = {},
): Promise<void> {
  const invocation = buildDedicatedWorkerInvocation(options);
  const environment = workerEnvironment(
    dependencies.environment ?? process.env,
  );
  const result = await (dependencies.spawn ?? spawnWorker)(
    invocation.command,
    invocation.args,
    {
      env: environment,
      signal: dependencies.signal,
    },
  );
  if (result.code !== 0) {
    throw new Error(
      `Dedicated worker for job ${options.jobId} ${getWorkerExitOutcome(result)}`,
    );
  }
}
