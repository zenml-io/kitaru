import {
  type JobResponse,
  KitaruApiError,
  type Page,
  type TaskResponse,
} from "@zenml-io/kitaru";

import type { CancellationRecord, RunManifestStore } from "./run-state.js";
import { runDedicatedWorker } from "./worker.js";

const TERMINAL_JOB_STATUSES = new Set(["completed", "failed", "canceled"]);

export interface JobManagementClient {
  jobs: {
    cancel(jobId: string): Promise<JobResponse>;
    get(jobId: string): Promise<JobResponse>;
    listTasks(jobId: string): Promise<Page<TaskResponse>>;
    wait(jobId: string): Promise<JobResponse>;
  };
}

export interface VerifyOwnedJobOptions {
  client: JobManagementClient;
  expectedAgentVersionId: string;
  expectedKind: JobResponse["kind"];
  jobId: string;
  observedJob?: JobResponse;
  ownerId: string;
}

export interface RunOwnedJobOptions extends VerifyOwnedJobOptions {
  apiUrl?: string;
  runWorker?: typeof runDedicatedWorker;
  store: RunManifestStore;
}

export class WorkerJobError extends Error {
  readonly cancellation: CancellationRecord;

  constructor(jobId: string, cause: unknown, cancellation: CancellationRecord) {
    super(`Dedicated worker failed for Kitaru job ${jobId}`, { cause });
    this.name = "WorkerJobError";
    this.cancellation = cancellation;
  }
}

export async function verifyOwnedJob(
  options: VerifyOwnedJobOptions,
): Promise<JobResponse> {
  const job =
    options.observedJob ?? (await options.client.jobs.get(options.jobId));
  if (job.id !== options.jobId) {
    throw new Error(`Kitaru returned the wrong job for ${options.jobId}`);
  }
  if (job.owner_id !== options.ownerId) {
    throw new Error(
      `Kitaru job ${options.jobId} does not belong to account ${options.ownerId}`,
    );
  }
  if (job.kind !== options.expectedKind) {
    throw new Error(
      `Kitaru job ${options.jobId} has kind ${job.kind}, expected ${options.expectedKind}`,
    );
  }
  const tasks = await options.client.jobs.listTasks(options.jobId);
  if (tasks.next_cursor !== null) {
    throw new Error(
      `Kitaru job ${options.jobId} has more tasks than this demo can verify safely`,
    );
  }
  if (tasks.items.some((task) => task.job_id !== options.jobId)) {
    throw new Error(
      `Kitaru job ${options.jobId} returned a task from another job`,
    );
  }
  const agentTasks = tasks.items.filter((task) => task.kind === "agent");
  if (
    agentTasks.length !== 1 ||
    agentTasks[0]?.agent_version_id !== options.expectedAgentVersionId
  ) {
    throw new Error(
      `Kitaru job ${options.jobId} does not contain the expected agent task`,
    );
  }
  return job;
}

export async function verifyReplaceableJob(
  options: VerifyOwnedJobOptions,
): Promise<JobResponse> {
  const job = await verifyOwnedJob(options);
  if (job.status !== "failed" && job.status !== "canceled") {
    throw new Error(
      `Job ${job.id} must be failed or canceled before replacement`,
    );
  }
  return job;
}

async function recordCancellation(
  store: RunManifestStore,
  cancellation: CancellationRecord,
): Promise<void> {
  const manifest = await store.read();
  manifest.cancellations.push(cancellation);
  await store.save(manifest);
}

async function cancelAfterWorkerFailure(
  options: RunOwnedJobOptions,
): Promise<CancellationRecord> {
  const cancellation: CancellationRecord = {
    attempted_at: new Date().toISOString(),
    job_id: options.jobId,
    reconciled_after_error: false,
    state: "requested",
  };
  try {
    await recordCancellation(options.store, cancellation);
  } catch {
    cancellation.state = "ambiguous";
    cancellation.error_kind = "cancellation_journal_error";
    return cancellation;
  }
  try {
    const canceled = await options.client.jobs.cancel(options.jobId);
    cancellation.observed_status = canceled.status;
    cancellation.state = TERMINAL_JOB_STATUSES.has(canceled.status)
      ? "terminal"
      : "accepted";
  } catch (error) {
    cancellation.reconciled_after_error = true;
    cancellation.error_kind =
      error instanceof KitaruApiError ? error.kind : "cancellation_error";
    cancellation.error_status =
      error instanceof KitaruApiError ? error.status : null;
    try {
      const observed = await options.client.jobs.get(options.jobId);
      cancellation.observed_status = observed.status;
      if (TERMINAL_JOB_STATUSES.has(observed.status)) {
        cancellation.state = "terminal";
      } else if (observed.cancel_requested_at != null) {
        cancellation.state = "accepted";
      } else {
        cancellation.state = "ambiguous";
      }
    } catch {
      cancellation.state = "ambiguous";
    }
  }
  try {
    const manifest = await options.store.read();
    const recorded = manifest.cancellations.findLast(
      ({ attempted_at, job_id }) =>
        job_id === options.jobId && attempted_at === cancellation.attempted_at,
    );
    if (recorded !== undefined) {
      Object.assign(recorded, cancellation);
      await options.store.save(manifest);
    }
  } catch {
    // Keep the worker failure primary if the cancellation journal cannot update.
    cancellation.state = "ambiguous";
    cancellation.error_kind = "cancellation_journal_error";
  }
  return cancellation;
}

export async function runOwnedJob(
  options: RunOwnedJobOptions,
): Promise<JobResponse> {
  const initial = await verifyOwnedJob(options);
  if (TERMINAL_JOB_STATUSES.has(initial.status)) {
    if (initial.status !== "completed") {
      throw new Error(
        `Kitaru job ${initial.id} settled as ${initial.status}: ${initial.error ?? "no error detail"}`,
      );
    }
    return initial;
  }
  if (initial.cancel_requested_at !== null) {
    const settled = await options.client.jobs.wait(options.jobId);
    if (settled.status !== "completed") {
      throw new Error(
        `Kitaru job ${settled.id} settled as ${settled.status}: ${settled.error ?? "no error detail"}`,
      );
    }
    return settled;
  }
  try {
    await (options.runWorker ?? runDedicatedWorker)({
      apiUrl: options.apiUrl,
      jobId: options.jobId,
      stateDir: options.store.stateDir,
    });
  } catch (error) {
    const cancellation = await cancelAfterWorkerFailure(options);
    throw new WorkerJobError(options.jobId, error, cancellation);
  }
  const settled = await options.client.jobs.wait(options.jobId);
  if (settled.status !== "completed") {
    throw new Error(
      `Kitaru job ${settled.id} settled as ${settled.status}: ${settled.error ?? "no error detail"}`,
    );
  }
  return settled;
}
