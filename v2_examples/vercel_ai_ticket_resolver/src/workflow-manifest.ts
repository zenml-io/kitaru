import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import type { ModelProvider } from "./preflight.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const MANIFEST_NAME = "workflow.json";
const STAGE_NAMES = [
  "baseline",
  "review",
  "baseline_evaluation",
  "cohorts",
  "experiment_runs",
  "verification",
] as const;
const WORKFLOW_STAGE_STATUSES = [
  "planned",
  "submitted",
  "committed",
  "awaiting_worker",
  "completed",
  "failed",
  "ambiguous",
] as const;
const WORKFLOW_STAGE_STATUS_SET = new Set<string>(WORKFLOW_STAGE_STATUSES);

export type WorkflowStageName = (typeof STAGE_NAMES)[number];
export type WorkflowStageStatus = (typeof WORKFLOW_STAGE_STATUSES)[number];
export type WorkflowPhase = WorkflowStageName | "completed";

export interface WorkflowStage {
  status: WorkflowStageStatus;
}

export interface PendingOperation {
  fingerprint: string;
  key: string;
  kind: string;
  parent_ids: Record<string, string>;
  status: "submitted" | "ambiguous";
}

export interface WorkflowIds {
  agent_id: string | null;
  agent_versions: { baseline: string | null; strict: string | null };
  annotation_ids: string[];
  baseline_sessions: Record<string, string>;
  cohort_versions: { control: string | null; target: string | null };
  cohorts: { control: string | null; target: string | null };
  evaluation_ids: string[];
  evaluation_job_id: string | null;
  evaluator_blob_id: string | null;
  evaluator_id: string | null;
  evaluator_version_id: string | null;
  experiment_id: string | null;
  experiment_run_ids: { control: string | null; target: string | null };
  investigation_id: string | null;
  investigation_session_id: string | null;
  replay_evaluation_ids: string[];
  replay_ids: string[];
  replay_job_ids: string[];
  replay_result_session_ids: string[];
  task_ids: string[];
}

export interface WorkflowManifest {
  evidence_set_id: string;
  ids: WorkflowIds;
  pending_operation: PendingOperation | null;
  phase: WorkflowPhase;
  provider: {
    fixture_version: "returns-v1";
    kind: ModelProvider;
    provider_call: boolean;
    requested_model: "openai/gpt-5-nano";
    served_model: "kitaru-returns-scripted-fixture" | "openai/gpt-5-nano";
    synthetic_usage: boolean;
  };
  schema_version: 2;
  server: {
    account_id: string;
    api_url: string;
    auth_scheme: "none" | "local" | "control_plane";
    version: string;
  };
  source_hashes: {
    baseline_instructions_sha256: string;
    evaluator_sha256: string;
    fixtures_sha256: string;
    strict_instructions_sha256: string;
  };
  stages: Record<WorkflowStageName, WorkflowStage>;
}

interface CreateWorkflowManifestInput {
  accountId: string;
  apiUrl: string;
  authScheme: WorkflowManifest["server"]["auth_scheme"];
  evidenceSetId?: string;
  provider?: ModelProvider;
  serverVersion: string;
  sourceHashes: WorkflowManifest["source_hashes"];
}

function getProviderDetails(kind: ModelProvider): WorkflowManifest["provider"] {
  return {
    fixture_version: "returns-v1",
    kind,
    provider_call: kind === "openai",
    requested_model: "openai/gpt-5-nano",
    served_model:
      kind === "deterministic"
        ? "kitaru-returns-scripted-fixture"
        : "openai/gpt-5-nano",
    synthetic_usage: kind === "deterministic",
  };
}

function hasValidProvider(value: unknown): boolean {
  if (
    !isRecord(value) ||
    !["deterministic", "openai"].includes(String(value.kind))
  ) {
    return false;
  }
  const expected = getProviderDetails(value.kind as ModelProvider);
  return (
    hasOnlyKeys(value, Object.keys(expected)) &&
    Object.entries(expected).every(
      ([key, expectedValue]) => value[key] === expectedValue,
    )
  );
}

function createEmptyIds(): WorkflowIds {
  return {
    agent_id: null,
    agent_versions: { baseline: null, strict: null },
    annotation_ids: [],
    baseline_sessions: {},
    cohort_versions: { control: null, target: null },
    cohorts: { control: null, target: null },
    evaluation_ids: [],
    evaluation_job_id: null,
    evaluator_blob_id: null,
    evaluator_id: null,
    evaluator_version_id: null,
    experiment_id: null,
    experiment_run_ids: { control: null, target: null },
    investigation_id: null,
    investigation_session_id: null,
    replay_evaluation_ids: [],
    replay_ids: [],
    replay_job_ids: [],
    replay_result_session_ids: [],
    task_ids: [],
  };
}

export function createWorkflowManifest(
  input: CreateWorkflowManifestInput,
): WorkflowManifest {
  const provider = input.provider ?? "deterministic";
  return {
    evidence_set_id: input.evidenceSetId ?? randomUUID(),
    ids: createEmptyIds(),
    pending_operation: null,
    phase: "baseline",
    provider: getProviderDetails(provider),
    schema_version: 2,
    server: {
      account_id: input.accountId,
      api_url: input.apiUrl,
      auth_scheme: input.authScheme,
      version: input.serverVersion,
    },
    source_hashes: input.sourceHashes,
    stages: Object.fromEntries(
      STAGE_NAMES.map((name) => [name, { status: "planned" }]),
    ) as Record<WorkflowStageName, WorkflowStage>,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, keys: readonly string[]) {
  return (
    Object.keys(value).length === keys.length &&
    Object.keys(value).every((key) => keys.includes(key))
  );
}

function hasValidStages(value: unknown): boolean {
  if (!isRecord(value) || !hasOnlyKeys(value, STAGE_NAMES)) {
    return false;
  }
  return STAGE_NAMES.every((name) => {
    const stage = value[name];
    return (
      isRecord(stage) && WORKFLOW_STAGE_STATUS_SET.has(String(stage.status))
    );
  });
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" && UUID_PATTERN.test(value);
}

function isUuidOrNull(value: unknown): value is string | null {
  return value === null || isUuid(value);
}

function isUuidArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isUuid);
}

function isUuidMap(value: unknown): value is Record<string, string> {
  return isRecord(value) && Object.values(value).every(isUuid);
}

function hasValidIds(value: unknown): boolean {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "agent_id",
      "agent_versions",
      "annotation_ids",
      "baseline_sessions",
      "cohort_versions",
      "cohorts",
      "evaluation_ids",
      "evaluation_job_id",
      "evaluator_blob_id",
      "evaluator_id",
      "evaluator_version_id",
      "experiment_id",
      "experiment_run_ids",
      "investigation_id",
      "investigation_session_id",
      "replay_evaluation_ids",
      "replay_ids",
      "replay_job_ids",
      "replay_result_session_ids",
      "task_ids",
    ]) ||
    !isRecord(value.agent_versions) ||
    !hasOnlyKeys(value.agent_versions, ["baseline", "strict"]) ||
    !isRecord(value.cohort_versions) ||
    !hasOnlyKeys(value.cohort_versions, ["control", "target"]) ||
    !isRecord(value.cohorts) ||
    !hasOnlyKeys(value.cohorts, ["control", "target"]) ||
    !isRecord(value.experiment_run_ids) ||
    !hasOnlyKeys(value.experiment_run_ids, ["control", "target"])
  ) {
    return false;
  }
  return (
    isUuidOrNull(value.agent_id) &&
    isUuidOrNull(value.agent_versions.baseline) &&
    isUuidOrNull(value.agent_versions.strict) &&
    isUuidArray(value.annotation_ids) &&
    isUuidMap(value.baseline_sessions) &&
    isUuidOrNull(value.cohort_versions.control) &&
    isUuidOrNull(value.cohort_versions.target) &&
    isUuidOrNull(value.cohorts.control) &&
    isUuidOrNull(value.cohorts.target) &&
    isUuidArray(value.evaluation_ids) &&
    isUuidOrNull(value.evaluation_job_id) &&
    isUuidOrNull(value.evaluator_blob_id) &&
    isUuidOrNull(value.evaluator_id) &&
    isUuidOrNull(value.evaluator_version_id) &&
    isUuidOrNull(value.experiment_id) &&
    isUuidOrNull(value.experiment_run_ids.control) &&
    isUuidOrNull(value.experiment_run_ids.target) &&
    isUuidOrNull(value.investigation_id) &&
    isUuidOrNull(value.investigation_session_id) &&
    isUuidArray(value.replay_evaluation_ids) &&
    isUuidArray(value.replay_ids) &&
    isUuidArray(value.replay_job_ids) &&
    isUuidArray(value.replay_result_session_ids) &&
    isUuidArray(value.task_ids)
  );
}

function hasValidPendingOperation(value: unknown): boolean {
  if (value === null) {
    return true;
  }
  return (
    isRecord(value) &&
    hasOnlyKeys(value, [
      "fingerprint",
      "key",
      "kind",
      "parent_ids",
      "status",
    ]) &&
    typeof value.fingerprint === "string" &&
    SHA256_PATTERN.test(value.fingerprint) &&
    typeof value.key === "string" &&
    value.key.length > 0 &&
    typeof value.kind === "string" &&
    value.kind.length > 0 &&
    isUuidMap(value.parent_ids) &&
    (value.status === "submitted" || value.status === "ambiguous")
  );
}

function parseManifest(value: unknown): WorkflowManifest {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "evidence_set_id",
      "ids",
      "pending_operation",
      "phase",
      "provider",
      "schema_version",
      "server",
      "source_hashes",
      "stages",
    ]) ||
    value.schema_version !== 2 ||
    !isUuid(value.evidence_set_id) ||
    !isRecord(value.server) ||
    !hasOnlyKeys(value.server, [
      "account_id",
      "api_url",
      "auth_scheme",
      "version",
    ]) ||
    !isUuid(value.server.account_id) ||
    typeof value.server.api_url !== "string" ||
    typeof value.server.version !== "string" ||
    !["none", "local", "control_plane"].includes(
      String(value.server.auth_scheme),
    ) ||
    !hasValidProvider(value.provider) ||
    !isRecord(value.source_hashes) ||
    !hasOnlyKeys(value.source_hashes, [
      "baseline_instructions_sha256",
      "evaluator_sha256",
      "fixtures_sha256",
      "strict_instructions_sha256",
    ]) ||
    !Object.values(value.source_hashes).every(
      (hash) => typeof hash === "string" && SHA256_PATTERN.test(hash),
    ) ||
    !hasValidStages(value.stages) ||
    ![...STAGE_NAMES, "completed"].includes(
      String(value.phase) as WorkflowPhase,
    ) ||
    !hasValidPendingOperation(value.pending_operation) ||
    !hasValidIds(value.ids)
  ) {
    throw new Error(
      "workflow manifest is invalid or not allowlisted workflow manifest data",
    );
  }
  return value as unknown as WorkflowManifest;
}

async function readOptionalFile(path: string): Promise<string | undefined> {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if (
      error instanceof Error &&
      "code" in error &&
      (error as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return undefined;
    }
    throw error;
  }
}

export async function loadWorkflowManifest(
  stateDirectory = resolve(".state"),
): Promise<WorkflowManifest | undefined> {
  const contents = await readOptionalFile(join(stateDirectory, MANIFEST_NAME));
  if (contents === undefined) {
    return undefined;
  }
  try {
    return parseManifest(JSON.parse(contents) as unknown);
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error("workflow manifest is not valid JSON", { cause: error });
    }
    throw error;
  }
}

export class WorkflowManifestStore {
  readonly #directory: string;
  readonly #path: string;

  constructor(stateDirectory = resolve(".state")) {
    this.#directory = resolve(stateDirectory);
    this.#path = join(this.#directory, MANIFEST_NAME);
  }

  async load(): Promise<WorkflowManifest | undefined> {
    return loadWorkflowManifest(this.#directory);
  }

  async save(manifest: WorkflowManifest): Promise<void> {
    const parsed = parseManifest(manifest);
    await mkdir(dirname(this.#path), { recursive: true, mode: 0o700 });
    await chmod(this.#directory, 0o700);
    const temporary = `${this.#path}.${randomUUID()}.tmp`;
    await writeFile(temporary, `${JSON.stringify(parsed, null, 2)}\n`, {
      encoding: "utf8",
      flag: "wx",
      mode: 0o600,
    });
    await rename(temporary, this.#path);
    await chmod(this.#path, 0o600);
  }

  async archive(manifest: WorkflowManifest): Promise<void> {
    const archive = join(
      this.#directory,
      "evidence-sets",
      `${manifest.evidence_set_id}.json`,
    );
    await mkdir(dirname(archive), { recursive: true, mode: 0o700 });
    const parsed = parseManifest(manifest);
    const contents = `${JSON.stringify(parsed, null, 2)}\n`;
    try {
      await writeFile(archive, contents, {
        encoding: "utf8",
        flag: "wx",
        mode: 0o600,
      });
    } catch (error) {
      if (
        !(error instanceof Error) ||
        !("code" in error) ||
        (error as NodeJS.ErrnoException).code !== "EEXIST"
      ) {
        throw error;
      }
      try {
        const archived = parseManifest(
          JSON.parse(await readFile(archive, "utf8")) as unknown,
        );
        if (JSON.stringify(archived) === JSON.stringify(parsed)) {
          return;
        }
      } catch (cause) {
        throw new Error(
          `Workflow archive collision for evidence set ${manifest.evidence_set_id}`,
          { cause },
        );
      }
      throw new Error(
        `Workflow archive collision for evidence set ${manifest.evidence_set_id}`,
      );
    }
  }
}
