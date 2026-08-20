import { createHash, randomUUID } from "node:crypto";
import {
  chmod,
  constants,
  lstat,
  mkdir,
  open,
  readFile,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { hostname } from "node:os";
import { basename, dirname, join, resolve } from "node:path";

export const MANIFEST_NAME = "run-manifest.json";
const RUN_LOCK_DIRECTORY_NAME = "run.lock";
const RUN_LOCK_OWNER_NAME = "owner.json";
const MAX_MANIFEST_BYTES = 1024 * 1024;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const DEMO_STATUS_VALUES = [
  "preparing",
  "recording",
  "replaying",
  "completed",
] as const;
const OPERATION_KIND_VALUES = [
  "create_agent",
  "create_agent_version",
  "create_evaluator",
  "upload_evaluator_source",
  "create_evaluator_version",
  "create_initial_job",
  "create_replay",
] as const;
const DEMO_STATUSES = new Set<string>(DEMO_STATUS_VALUES);
const OPERATION_KINDS = new Set<string>(OPERATION_KIND_VALUES);

export type DemoStatus = (typeof DEMO_STATUS_VALUES)[number];
export type OperationKind = (typeof OPERATION_KIND_VALUES)[number];

export interface OperationRecord {
  completed_at?: string;
  fingerprint: string;
  kind: OperationKind;
  remote_ids: string[];
  started_at: string;
  state: "planned" | "committed" | "ambiguous" | "retried";
}

export interface CreateRemoteOptions<T> {
  adopt?: (remoteId: string) => Promise<T>;
  adoptionId?: string;
  commit?: (manifest: RunManifest, remote: T) => void;
  retry?: boolean;
  validateAdopted?: (remote: T) => Promise<void> | void;
}

export interface CancellationRecord {
  attempted_at: string;
  error_kind?: string;
  error_status?: number | null;
  job_id: string;
  observed_status?: string;
  reconciled_after_error: boolean;
  state: "requested" | "accepted" | "terminal" | "ambiguous";
}

export interface RunResources {
  agent_id?: string;
  agent_version_id?: string;
  evaluator_blob_id?: string;
  evaluator_id?: string;
  evaluator_version_id?: string;
  initial_job_id?: string;
  initial_session_id?: string;
  replay_id?: string;
  replay_job_id?: string;
  result_session_id?: string;
}

export interface DemoSummary {
  initial_outbox_count: number;
  replay_outbox_count: number;
}

export interface RunManifest {
  cancellations: CancellationRecord[];
  created_at: string;
  operations: OperationRecord[];
  owner_id: string;
  resources: RunResources;
  run_id: string;
  schema_version: 1;
  server_url: string;
  status: DemoStatus;
  summary?: DemoSummary;
  updated_at: string;
}

export interface CreateRunManifestOptions {
  ownerId: string;
  rootDir: string;
  runId?: string;
  serverUrl: string;
}

interface RunLockOwner {
  hostname: string;
  pid: number;
  started_at: string;
  token: string;
}

export interface RunLock {
  release(): Promise<void>;
}

export class AmbiguousOperationError extends Error {
  readonly operation: OperationRecord;

  constructor(operation: OperationRecord, options?: ErrorOptions) {
    super(
      `Kitaru operation ${operation.kind} is ambiguous. Inspect the server, then resume with --adopt ${operation.kind}=UUID or --retry ${operation.kind}.`,
      options,
    );
    this.name = "AmbiguousOperationError";
    this.operation = operation;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isUuid(value: unknown): value is string {
  return typeof value === "string" && UUID_PATTERN.test(value);
}

export function parseOperationKind(value: string): OperationKind {
  if (!OPERATION_KINDS.has(value as OperationKind)) {
    throw new Error(`Unknown Kitaru operation: ${value}`);
  }
  return value as OperationKind;
}

function parseManifest(value: unknown): RunManifest {
  if (!isRecord(value) || value.schema_version !== 1) {
    throw new Error("Run manifest must use schema version 1");
  }
  for (const field of ["server_url", "created_at", "updated_at"]) {
    if (typeof value[field] !== "string" || value[field].length === 0) {
      throw new Error(`Run manifest has an invalid ${field}`);
    }
  }
  if (!isUuid(value.run_id) || !isUuid(value.owner_id)) {
    throw new Error("Run manifest has an invalid run or owner id");
  }
  if (
    typeof value.status !== "string" ||
    !DEMO_STATUSES.has(value.status as DemoStatus)
  ) {
    throw new Error("Run manifest has an invalid status");
  }
  try {
    const server = new URL(value.server_url as string);
    if (!["http:", "https:"].includes(server.protocol)) {
      throw new Error("unsupported protocol");
    }
  } catch (error) {
    throw new Error("Run manifest has an invalid server_url", { cause: error });
  }
  if (
    !isRecord(value.resources) ||
    !Array.isArray(value.operations) ||
    !Array.isArray(value.cancellations)
  ) {
    throw new Error("Run manifest has invalid lifecycle state");
  }
  for (const [field, resourceId] of Object.entries(value.resources)) {
    if (!isUuid(resourceId)) {
      throw new Error(`Run manifest has an invalid resource id for ${field}`);
    }
  }
  for (const operation of value.operations) {
    if (
      !isRecord(operation) ||
      typeof operation.kind !== "string" ||
      !OPERATION_KINDS.has(operation.kind as OperationKind) ||
      typeof operation.fingerprint !== "string" ||
      !/^sha256:[0-9a-f]{64}$/i.test(operation.fingerprint) ||
      !Array.isArray(operation.remote_ids) ||
      !operation.remote_ids.every(isUuid) ||
      !["planned", "committed", "ambiguous", "retried"].includes(
        String(operation.state),
      )
    ) {
      throw new Error("Run manifest has an invalid operation journal");
    }
  }
  for (const cancellation of value.cancellations) {
    if (
      !isRecord(cancellation) ||
      !isUuid(cancellation.job_id) ||
      typeof cancellation.attempted_at !== "string" ||
      typeof cancellation.reconciled_after_error !== "boolean" ||
      !["requested", "accepted", "terminal", "ambiguous"].includes(
        String(cancellation.state),
      ) ||
      (cancellation.observed_status !== undefined &&
        typeof cancellation.observed_status !== "string") ||
      (cancellation.error_kind !== undefined &&
        typeof cancellation.error_kind !== "string") ||
      (cancellation.error_status !== undefined &&
        cancellation.error_status !== null &&
        typeof cancellation.error_status !== "number")
    ) {
      throw new Error("Run manifest has an invalid cancellation record");
    }
  }
  if (value.status === "completed" && value.summary === undefined) {
    throw new Error("Completed run manifest is missing its summary");
  }
  if (value.summary !== undefined) {
    if (
      !isRecord(value.summary) ||
      typeof value.summary.initial_outbox_count !== "number" ||
      typeof value.summary.replay_outbox_count !== "number"
    ) {
      throw new Error("Run manifest has an invalid completed summary");
    }
  }
  return value as unknown as RunManifest;
}

function getRemoteIds(value: unknown): string[] {
  if (!isRecord(value)) {
    return [];
  }
  return [value.id, value.job_id].filter(
    (candidate): candidate is string => typeof candidate === "string",
  );
}

function parseLockOwner(value: unknown): RunLockOwner | undefined {
  if (
    !isRecord(value) ||
    typeof value.hostname !== "string" ||
    !Number.isInteger(value.pid) ||
    typeof value.started_at !== "string" ||
    typeof value.token !== "string"
  ) {
    return undefined;
  }
  return value as unknown as RunLockOwner;
}

async function readLockOwner(
  lockDirectory: string,
): Promise<RunLockOwner | undefined> {
  try {
    return parseLockOwner(
      JSON.parse(
        await readFile(join(lockDirectory, RUN_LOCK_OWNER_NAME), "utf8"),
      ) as unknown,
    );
  } catch (error) {
    if (
      error instanceof Error &&
      (("code" in error &&
        (error as NodeJS.ErrnoException).code === "ENOENT") ||
        error instanceof SyntaxError)
    ) {
      return undefined;
    }
    throw error;
  }
}

function processIsRunning(owner: RunLockOwner): boolean {
  if (owner.hostname !== hostname()) {
    return true;
  }
  try {
    process.kill(owner.pid, 0);
    return true;
  } catch (error) {
    return !(
      error instanceof Error &&
      "code" in error &&
      (error as NodeJS.ErrnoException).code === "ESRCH"
    );
  }
}

async function writeAtomically(
  path: string,
  value: RunManifest,
): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await chmod(temporary, 0o600);
  await rename(temporary, path);
}

export function operationFingerprint(value: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

export class RunManifestStore {
  readonly path: string;
  readonly stateDir: string;

  private constructor(stateDir: string) {
    this.stateDir = stateDir;
    this.path = join(stateDir, MANIFEST_NAME);
  }

  static async create(
    options: CreateRunManifestOptions,
  ): Promise<RunManifestStore> {
    const runId = options.runId ?? randomUUID();
    if (!isUuid(runId)) {
      throw new Error("runId must be a UUID");
    }
    const rootDir = resolve(options.rootDir);
    const stateDir = join(rootDir, runId);
    await mkdir(rootDir, { mode: 0o700, recursive: true });
    await mkdir(stateDir, { mode: 0o700 });
    await chmod(stateDir, 0o700);
    const store = new RunManifestStore(stateDir);
    const now = new Date().toISOString();
    const manifest: RunManifest = {
      cancellations: [],
      created_at: now,
      operations: [],
      owner_id: options.ownerId,
      resources: {},
      run_id: runId,
      schema_version: 1,
      server_url: options.serverUrl.replace(/\/+$/, ""),
      status: "preparing",
      updated_at: now,
    };
    const handle = await open(store.path, "wx", 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(manifest, null, 2)}\n`, "utf8");
    } finally {
      await handle.close();
    }
    return store;
  }

  static open(stateDir: string): RunManifestStore {
    return new RunManifestStore(resolve(stateDir));
  }

  async read(): Promise<RunManifest> {
    let raw: unknown;
    try {
      const metadata = await lstat(this.path);
      if (
        metadata.isSymbolicLink() ||
        !metadata.isFile() ||
        metadata.size > MAX_MANIFEST_BYTES ||
        (process.platform !== "win32" &&
          ((metadata.mode & 0o077) !== 0 ||
            (process.getuid !== undefined &&
              metadata.uid !== process.getuid())))
      ) {
        throw new Error(
          "Run manifest must be a bounded owner-only regular file",
        );
      }
      const noFollow = process.platform === "win32" ? 0 : constants.O_NOFOLLOW;
      const handle = await open(this.path, constants.O_RDONLY | noFollow);
      try {
        raw = JSON.parse(await handle.readFile("utf8"));
      } finally {
        await handle.close();
      }
    } catch (error) {
      throw new Error(`Cannot read run manifest at ${this.path}`, {
        cause: error,
      });
    }
    const manifest = parseManifest(raw);
    if (basename(this.stateDir) !== manifest.run_id) {
      throw new Error("Run manifest id does not match its state directory");
    }
    return manifest;
  }

  async save(manifest: RunManifest): Promise<void> {
    manifest.updated_at = new Date().toISOString();
    await mkdir(dirname(this.path), { mode: 0o700, recursive: true });
    await writeAtomically(this.path, manifest);
  }

  async acquireRunLock(): Promise<RunLock> {
    const lockDirectory = join(this.stateDir, RUN_LOCK_DIRECTORY_NAME);
    const owner: RunLockOwner = {
      hostname: hostname(),
      pid: process.pid,
      started_at: new Date().toISOString(),
      token: randomUUID(),
    };

    while (true) {
      const candidate = `${lockDirectory}.${owner.token}.tmp`;
      await mkdir(candidate, { mode: 0o700 });
      await writeFile(
        join(candidate, RUN_LOCK_OWNER_NAME),
        `${JSON.stringify(owner)}\n`,
        { encoding: "utf8", flag: "wx", mode: 0o600 },
      );
      try {
        await rename(candidate, lockDirectory);
        break;
      } catch (error) {
        await rm(candidate, { force: true, recursive: true });
        if (
          !(
            error instanceof Error &&
            "code" in error &&
            ["EEXIST", "ENOTEMPTY"].includes(
              String((error as NodeJS.ErrnoException).code),
            )
          )
        ) {
          throw error;
        }
      }

      const existing = await readLockOwner(lockDirectory);
      if (existing !== undefined && processIsRunning(existing)) {
        throw new Error(
          `Run ${basename(this.stateDir)} is already active in process ${existing.pid}`,
        );
      }
      const stale = `${lockDirectory}.stale.${randomUUID()}`;
      try {
        await rename(lockDirectory, stale);
      } catch (error) {
        if (
          error instanceof Error &&
          "code" in error &&
          (error as NodeJS.ErrnoException).code === "ENOENT"
        ) {
          continue;
        }
        throw error;
      }
      await rm(stale, { force: true, recursive: true });
    }

    let released = false;
    return {
      release: async () => {
        if (released) return;
        const current = await readLockOwner(lockDirectory);
        if (current?.token !== owner.token) {
          throw new Error("Run lock ownership changed before release");
        }
        await rm(lockDirectory, { recursive: true });
        released = true;
      },
    };
  }

  async withRunLock<T>(action: () => Promise<T>): Promise<T> {
    const lock = await this.acquireRunLock();
    try {
      return await action();
    } finally {
      await lock.release();
    }
  }

  async authorizeReplacement(
    kind: "create_initial_job" | "create_replay",
    fingerprint: string,
    remoteIds: readonly string[],
  ): Promise<void> {
    const manifest = await this.read();
    const lastOperation = manifest.operations.at(-1);
    const previousOperation = manifest.operations.at(-2);
    const matchesReplacement = (operation: OperationRecord | undefined) =>
      operation?.kind === kind &&
      operation.fingerprint === fingerprint &&
      operation.remote_ids.length === remoteIds.length &&
      operation.remote_ids.every((id, index) => id === remoteIds[index]);
    if (
      (lastOperation?.state === "retried" &&
        matchesReplacement(lastOperation)) ||
      ((lastOperation?.state === "planned" ||
        lastOperation?.state === "ambiguous") &&
        lastOperation.kind === kind &&
        lastOperation.fingerprint === fingerprint &&
        previousOperation?.state === "retried" &&
        matchesReplacement(previousOperation))
    ) {
      return;
    }
    if (
      lastOperation?.state !== "committed" ||
      !matchesReplacement(lastOperation)
    ) {
      throw new Error(
        `No exact committed ${kind} operation is available to replace`,
      );
    }
    lastOperation.completed_at = new Date().toISOString();
    lastOperation.state = "retried";
    await this.save(manifest);
  }

  async createRemote<T>(
    kind: OperationKind,
    fingerprint: string,
    request: () => Promise<T>,
    options: CreateRemoteOptions<T> = {},
  ): Promise<T> {
    const manifest = await this.read();
    const unresolved = manifest.operations.find(
      (operation) =>
        operation.state !== "committed" && operation.state !== "retried",
    );
    if (unresolved !== undefined) {
      if (unresolved.kind !== kind || unresolved.fingerprint !== fingerprint) {
        throw new Error(
          `Recovery for ${kind} does not match ambiguous ${unresolved.kind} operation`,
        );
      }
      if (options.adoptionId !== undefined && options.retry === true) {
        throw new Error(
          `Choose either adoption or retry for ${kind}, not both`,
        );
      }
      if (options.adoptionId !== undefined) {
        if (!isUuid(options.adoptionId)) {
          throw new Error(`Adoption id for ${kind} must be a UUID`);
        }
        if (
          options.adopt === undefined ||
          options.validateAdopted === undefined
        ) {
          throw new Error(`Operation ${kind} does not support safe adoption`);
        }
        const remote = await options.adopt(options.adoptionId);
        const ids = getRemoteIds(remote);
        if (ids[0] !== options.adoptionId) {
          throw new Error(
            `Kitaru returned the wrong resource for adopted ${kind} operation`,
          );
        }
        await options.validateAdopted(remote);
        unresolved.remote_ids = ids;
        unresolved.completed_at = new Date().toISOString();
        unresolved.state = "committed";
        options.commit?.(manifest, remote);
        await this.save(manifest);
        return remote;
      }
      if (options.retry !== true) {
        throw new AmbiguousOperationError(unresolved);
      }
      unresolved.completed_at = new Date().toISOString();
      unresolved.state = "retried";
      await this.save(manifest);
    } else if (options.adoptionId !== undefined) {
      throw new Error(`No ambiguous ${kind} operation is available to recover`);
    } else if (options.retry === true) {
      const retryAuthorization = manifest.operations.at(-1);
      if (
        retryAuthorization?.state !== "retried" ||
        retryAuthorization.kind !== kind ||
        retryAuthorization.fingerprint !== fingerprint
      ) {
        throw new Error(
          `No ambiguous or authorized ${kind} operation is available to retry`,
        );
      }
    }
    const operation: OperationRecord = {
      fingerprint,
      kind,
      remote_ids: [],
      started_at: new Date().toISOString(),
      state: "planned",
    };
    manifest.operations.push(operation);
    await this.save(manifest);
    let remote: T;
    try {
      remote = await request();
    } catch (error) {
      operation.state = "ambiguous";
      await this.save(manifest);
      throw new AmbiguousOperationError(operation, { cause: error });
    }
    try {
      operation.remote_ids = getRemoteIds(remote);
      operation.completed_at = new Date().toISOString();
      operation.state = "committed";
      options.commit?.(manifest, remote);
      await this.save(manifest);
    } catch (error) {
      operation.state = "ambiguous";
      await this.save(manifest).catch(() => undefined);
      throw new AmbiguousOperationError(operation, { cause: error });
    }
    return remote;
  }
}
