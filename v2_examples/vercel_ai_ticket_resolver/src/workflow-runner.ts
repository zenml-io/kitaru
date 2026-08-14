import { createHash } from "node:crypto";
import { mkdir, readFile, rename } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type {
  AgentResponse,
  AgentVersionResponse,
  AnnotationResponse,
  BlobResponse,
  CohortResponse,
  CohortVersionResponse,
  EvaluationResponse,
  EvaluatorResponse,
  EvaluatorVersionResponse,
  ExperimentResponse,
  ExperimentRunResponse,
  InvestigationResponse,
  JobResponse,
  KitaruClient,
  KitaruEnvironmentVariables,
  ReplayResponse,
} from "@zenml-io/kitaru";
import {
  createKitaruClient,
  readSelectedServerUrl,
  resolveConfigDirectory,
} from "@zenml-io/kitaru/node";

import { createTicketRun, instructionsFor } from "./agent.js";
import { ticketCases } from "./fixtures.js";
import { renderTicketPrompt } from "./models.js";
import {
  type ModelProvider,
  validateWorkflowEnvironment,
} from "./preflight.js";
import {
  createCompletedEvent,
  createWorkerHandoffEvent,
  type WorkerHandoffJob,
} from "./worker-handoff.js";
import {
  createWorkflowManifest,
  type PendingOperation,
  type WorkflowManifest,
  WorkflowManifestStore,
  type WorkflowStageName,
} from "./workflow-manifest.js";

export const BASELINE_FAILURE_TICKETS = ["ticket-004", "ticket-007"] as const;
export const TARGET_TICKETS = BASELINE_FAILURE_TICKETS;
export const CONTROL_TICKETS = [
  "ticket-001",
  "ticket-009",
  "ticket-010",
] as const;
export const TOOLS = [
  "lookup_order",
  "get_return_policy",
  "check_shipping",
  "issue_refund",
  "create_replacement",
  "escalate_to_human",
] as const;

const EXAMPLE_DIRECTORY = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
);
const TERMINAL_JOB_STATUSES = new Set(["completed", "failed", "canceled"]);
const COMPLETION_COUNTS = {
  baseline_failures: BASELINE_FAILURE_TICKETS.length,
  baseline_passes: ticketCases.length - BASELINE_FAILURE_TICKETS.length,
  baseline_sessions: ticketCases.length,
  control_sessions: CONTROL_TICKETS.length,
  experiment_runs: 2,
  replay_passes: TARGET_TICKETS.length + CONTROL_TICKETS.length,
  replays: TARGET_TICKETS.length + CONTROL_TICKETS.length,
  target_sessions: TARGET_TICKETS.length,
} as const;

export const workflowRequests = {
  createEvaluator: (name: string, version: number) => ({
    evaluators: [{ evaluator: name, version }],
  }),
  createExperimentRun: (agentVersionId: string, cohortVersionId: string) => ({
    agent_version_id: agentVersionId,
    cohort_version_id: cohortVersionId,
    evaluate_baselines: false,
  }),
  toolPolicy: { default: { type: "passthrough" as const } },
};

interface WorkflowArguments {
  adoptions: Record<string, string>;
  fresh: boolean;
  provider: ModelProvider;
  retries: Set<string>;
  stateDirectory: string;
}

export interface MutationInput<T> {
  adopt?: (id: string) => Promise<T>;
  afterRemoteCommit?: (value: T) => Promise<void>;
  commit: (manifest: WorkflowManifest, value: T) => void;
  create: () => Promise<T>;
  fingerprintInput: unknown;
  key: string;
  kind: string;
  manifest: WorkflowManifest;
  parentIds?: Record<string, string>;
  reconcile?: () => Promise<T[]>;
  stage: WorkflowStageName;
  store: WorkflowManifestStore;
  validate: (value: T) => void;
}

function normalizeStableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(normalizeStableValue);
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, item]) => [key, normalizeStableValue(item)]),
    );
  }
  return value;
}

function createSha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function createFingerprint(value: unknown): string {
  return createSha256(JSON.stringify(normalizeStableValue(value)));
}

function assertId(value: string | null | undefined, label: string): string {
  if (value == null) {
    throw new Error(`Workflow manifest is missing ${label}`);
  }
  return value;
}

function setStage(
  manifest: WorkflowManifest,
  stage: WorkflowStageName,
  status: WorkflowManifest["stages"][WorkflowStageName]["status"],
): void {
  manifest.phase = stage;
  manifest.stages[stage].status = status;
}

async function readSourceMaterial() {
  const read = (relative: string) =>
    readFile(join(EXAMPLE_DIRECTORY, relative), "utf8");
  const [readme, orders, policies, shipments, tickets] = await Promise.all([
    read("README.md"),
    read("fixtures/orders.v1.json"),
    read("fixtures/policies.v1.json"),
    read("fixtures/shipments.v1.json"),
    read("fixtures/tickets.v1.json"),
  ]);
  const match =
    /<!-- documented-evaluator:start -->\n```python\n([\s\S]*?)\n```\n<!-- documented-evaluator:end -->/.exec(
      readme,
    );
  if (!match?.[1]) {
    throw new Error(
      "README must contain one documented evaluator source block",
    );
  }
  const evaluatorSource = `${match[1]}\n`;
  return {
    evaluatorSource,
    hashes: {
      baseline_instructions_sha256: createSha256(instructionsFor("baseline")),
      evaluator_sha256: createSha256(evaluatorSource),
      fixtures_sha256: createSha256(
        [orders, policies, shipments, tickets].join("\0"),
      ),
      strict_instructions_sha256: createSha256(instructionsFor("strict")),
    },
  };
}

export interface WorkflowDependencies {
  createClient?: typeof createKitaruClient;
  readSourceMaterial?: typeof readSourceMaterial;
}

async function resolveApiUrl(
  environment: KitaruEnvironmentVariables,
): Promise<string> {
  const selected =
    environment.KITARU_API_URL ??
    (await readSelectedServerUrl(resolveConfigDirectory(environment)));
  return selected.replace(/\/+$/, "");
}

function samePending(
  pending: PendingOperation,
  expected: PendingOperation,
): boolean {
  return (
    pending.key === expected.key &&
    pending.kind === expected.kind &&
    pending.fingerprint === expected.fingerprint &&
    JSON.stringify(pending.parent_ids) === JSON.stringify(expected.parent_ids)
  );
}

export async function runJournaledMutation<T>(
  input: MutationInput<T>,
  recovery: Pick<WorkflowArguments, "adoptions" | "retries">,
): Promise<T> {
  const commitValue = async (
    value: T,
    afterValidation?: (value: T) => Promise<void>,
  ): Promise<T> => {
    input.validate(value);
    await afterValidation?.(value);
    input.commit(input.manifest, value);
    input.manifest.pending_operation = null;
    input.manifest.stages[input.stage].status = "committed";
    await input.store.save(input.manifest);
    return value;
  };
  const pending: PendingOperation = {
    fingerprint: createFingerprint(input.fingerprintInput),
    key: input.key,
    kind: input.kind,
    parent_ids: input.parentIds ?? {},
    status: "submitted",
  };
  const existing = input.manifest.pending_operation;
  if (existing !== null) {
    if (!samePending(existing, pending)) {
      throw new Error(
        `Workflow has unresolved operation ${existing.key}; cannot start ${input.key}`,
      );
    }
    const adoptedId = recovery.adoptions[input.key];
    if (adoptedId !== undefined) {
      if (input.adopt === undefined) {
        throw new Error(`Operation ${input.key} does not support adoption`);
      }
      const adopted = await input.adopt(adoptedId);
      return commitValue(adopted);
    }

    const retryRequested = recovery.retries.has(input.key);
    const retryAuthorized = existing.status === "retry_authorized";
    const retrySubmitted = existing.status === "retry_submitted";
    if (!retryAuthorized && !retrySubmitted && input.reconcile !== undefined) {
      const candidates = await input.reconcile();
      if (candidates.length === 1) {
        const reconciled = candidates[0] as T;
        return commitValue(reconciled);
      }
      if (candidates.length > 1) {
        existing.status = "ambiguous";
        input.manifest.stages[input.stage].status = "ambiguous";
        await input.store.save(input.manifest);
        throw new Error(
          `Operation ${input.key} is ambiguous. Inspect the exact remote object, then pass --adopt ${input.key}=ID.`,
        );
      }
    }
    if (retryRequested) {
      existing.status = "retry_authorized";
      input.manifest.stages[input.stage].status = "planned";
      await input.store.save(input.manifest);
    }
    if (retryRequested || retryAuthorized) {
      pending.status = "retry_submitted";
    } else {
      existing.status = "ambiguous";
      input.manifest.stages[input.stage].status = "ambiguous";
      await input.store.save(input.manifest);
      throw new Error(
        `Operation ${input.key} is ambiguous. Inspect the exact remote object, then pass --adopt ${input.key}=ID or --retry ${input.key}.`,
      );
    }
  }

  input.manifest.pending_operation = pending;
  setStage(input.manifest, input.stage, "submitted");
  await input.store.save(input.manifest);
  const created = await input.create();
  return commitValue(created, input.afterRemoteCommit);
}

function createNamedFilter(name: string) {
  return {
    filter: { field: "name", op: "eq" as const, value: name },
    size: 100,
  };
}

interface InvestigationReconciliationClient<
  T extends Pick<InvestigationResponse, "name">,
> {
  investigations: {
    iter(params: {
      filter: { field: "agent_id"; op: "eq"; value: string };
      size: number;
    }): AsyncIterable<T>;
  };
}

export async function findInvestigationsByName<
  T extends Pick<InvestigationResponse, "name">,
>(
  client: InvestigationReconciliationClient<T>,
  agentId: string,
  name: string,
): Promise<T[]> {
  const matches: T[] = [];
  for await (const investigation of client.investigations.iter({
    filter: { field: "agent_id", op: "eq", value: agentId },
    size: 100,
  })) {
    if (investigation.name === name) {
      matches.push(investigation);
      if (matches.length === 2) {
        break;
      }
    }
  }
  return matches;
}

async function ensureAgent(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
): Promise<AgentResponse> {
  if (manifest.ids.agent_id !== null) {
    return client.agents.get(manifest.ids.agent_id);
  }
  const name = `vercel-returns-${manifest.evidence_set_id.replaceAll("-", "").slice(0, 12)}`;
  const request = {
    name,
    description: "Synthetic TypeScript returns resolver workflow.",
  };
  return runJournaledMutation(
    {
      adopt: (id) => client.agents.get(id),
      commit: (state, value) => {
        state.ids.agent_id = value.id;
      },
      create: () => client.agents.create(request),
      fingerprintInput: request,
      key: "agent",
      kind: "agent",
      manifest,
      reconcile: async () =>
        (await client.agents.list(createNamedFilter(name))).items,
      stage: "baseline",
      store,
      validate: (value) => {
        if (value.name !== name) {
          throw new Error("Adopted agent does not match the workflow name");
        }
      },
    },
    recovery,
  );
}

async function ensureAgentVersion(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  mode: "baseline" | "strict",
): Promise<AgentVersionResponse> {
  const existing = manifest.ids.agent_versions[mode];
  if (existing !== null) {
    return client.agents.getVersion(existing);
  }
  const agentId = assertId(manifest.ids.agent_id, "agent_id");
  const displayVersion =
    mode === "baseline" ? "baseline-v1" : "strict-policy-v2";
  const request = {
    display_version: displayVersion,
    description:
      mode === "baseline"
        ? "Deterministic baseline returns policy."
        : "Require approval for risky or oversized refunds.",
    run_spec: {
      command: "node dist/main.js",
      working_dir: EXAMPLE_DIRECTORY,
      env: {
        KITARU_AGENT_ID: agentId,
        RETURNS_MODEL_PROVIDER: manifest.provider.kind,
        RETURNS_POLICY_MODE: mode,
      },
      timeout_seconds: 180,
    },
    capabilities: { tools: [...TOOLS] },
  };
  return runJournaledMutation(
    {
      adopt: (id) => client.agents.getVersion(id),
      commit: (state, value) => {
        state.ids.agent_versions[mode] = value.id;
      },
      create: () => client.agents.createVersion(agentId, request),
      fingerprintInput: request,
      key: `agent_version.${mode}`,
      kind: "agent_version",
      manifest,
      parentIds: { agent_id: agentId },
      reconcile: async () =>
        (await client.agents.listVersions(agentId, { size: 100 })).items.filter(
          (version) => version.display_version === displayVersion,
        ),
      stage: mode === "baseline" ? "baseline" : "cohorts",
      store,
      validate: (value) => {
        if (
          value.agent_id !== agentId ||
          value.display_version !== displayVersion
        ) {
          throw new Error("Adopted agent version does not match the workflow");
        }
      },
    },
    recovery,
  );
}

async function recordBaselines(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  environment: Readonly<Record<string, string | undefined>>,
): Promise<void> {
  const agentId = assertId(manifest.ids.agent_id, "agent_id");
  const versionId = assertId(
    manifest.ids.agent_versions.baseline,
    "baseline agent version",
  );
  const attemptsDirectory = join(
    recovery.stateDirectory,
    "attempts",
    manifest.evidence_set_id,
  );
  for (const { ticket } of ticketCases) {
    if (manifest.ids.baseline_sessions[ticket.ticket_id] !== undefined) {
      continue;
    }
    const key = `baseline_session.${ticket.ticket_id}`;
    const sessionIdFile = join(
      attemptsDirectory,
      `${ticket.ticket_id}.session-id`,
    );
    await mkdir(dirname(sessionIdFile), { recursive: true, mode: 0o700 });
    if (manifest.pending_operation?.key === key && recovery.retries.has(key)) {
      try {
        const oldSessionId = (await readFile(sessionIdFile, "utf8")).trim();
        const archiveDirectory = join(attemptsDirectory, "archived");
        await mkdir(archiveDirectory, { recursive: true, mode: 0o700 });
        await rename(
          sessionIdFile,
          join(
            archiveDirectory,
            `${ticket.ticket_id}.${oldSessionId}.session-id`,
          ),
        );
      } catch (error) {
        if (
          !(
            error instanceof Error &&
            "code" in error &&
            (error as NodeJS.ErrnoException).code === "ENOENT"
          )
        ) {
          throw error;
        }
      }
    }
    await runJournaledMutation(
      {
        adopt: (id) => client.sessions.get(id),
        commit: (state, value) => {
          state.ids.baseline_sessions[ticket.ticket_id] = value.id;
        },
        create: async () => {
          await createTicketRun({
            environment: {
              ...environment,
              KITARU_AGENT_ID: agentId,
              KITARU_AGENT_VERSION_ID: versionId,
              KITARU_SESSION_ID_FILE: sessionIdFile,
            },
            mode: "baseline",
            prompt: renderTicketPrompt(ticket),
            provider: manifest.provider.kind,
          }).generate();
          const sessionId = (await readFile(sessionIdFile, "utf8")).trim();
          return client.sessions.get(sessionId);
        },
        fingerprintInput: {
          agent_id: agentId,
          agent_version_id: versionId,
          prompt_sha256: createSha256(renderTicketPrompt(ticket)),
          provider: manifest.provider.kind,
          ticket_id: ticket.ticket_id,
        },
        key,
        kind: "baseline_session",
        manifest,
        parentIds: { agent_id: agentId, agent_version_id: versionId },
        stage: "baseline",
        store,
        validate: (value) => {
          if (
            value.agent_id !== agentId ||
            value.agent_version_id !== versionId ||
            value.status !== "completed"
          ) {
            throw new Error(
              `Session ${value.id} does not match completed baseline ${ticket.ticket_id}`,
            );
          }
        },
      },
      recovery,
    );
  }
  setStage(manifest, "baseline", "completed");
  await store.save(manifest);
}

function getAcceptedRefundNode(
  session: Awaited<ReturnType<KitaruClient["sessions"]["getWithNodes"]>>,
) {
  const candidates = session.nodes.filter(
    (node) =>
      node.node_type === "tool_call" &&
      node.status === "completed" &&
      node.tool_name === "issue_refund" &&
      typeof node.outputs === "object" &&
      node.outputs !== null &&
      !Array.isArray(node.outputs) &&
      (node.outputs as Record<string, unknown>).accepted === true,
  );
  if (candidates.length !== 1) {
    throw new Error("ticket-004 must have one accepted completed refund node");
  }
  return candidates[0] as (typeof candidates)[number];
}

async function ensureReview(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
): Promise<void> {
  if (manifest.stages.review.status === "completed") {
    return;
  }
  const agentId = assertId(manifest.ids.agent_id, "agent_id");
  const sessionId = manifest.ids.baseline_sessions["ticket-004"];
  if (sessionId === undefined) {
    throw new Error("Workflow is missing ticket-004 baseline session");
  }
  const fullSession = await client.sessions.getWithNodes(sessionId);
  const refundNode = getAcceptedRefundNode(fullSession);
  const name = `returns-review-${manifest.evidence_set_id.replaceAll("-", "").slice(0, 12)}`;
  const request = {
    agent_id: agentId,
    name,
    description: "Review whether risky refunds require human approval.",
    sessions: [
      {
        session_id: sessionId,
        questions: [
          {
            key: "outcome",
            question:
              "Is this outcome acceptable, problematic, or uncertain, and why?",
            highlights: [
              {
                selector: { node_id: refundNode.id, path: "/outputs" },
                description:
                  "A $280 refund exceeded the automatic approval threshold.",
              },
            ],
          },
          {
            key: "expected",
            question: "What should the agent have done instead?",
          },
          {
            key: "evidence",
            question:
              "Which policy condition and exact trace node support that judgment?",
            highlights: [
              {
                selector: { node_id: refundNode.id, path: "/outputs" },
                description:
                  "The accepted terminal tool output records the oversized refund.",
              },
            ],
          },
        ],
      },
    ],
  };
  let investigation: InvestigationResponse;
  if (manifest.ids.investigation_id === null) {
    investigation = await runJournaledMutation(
      {
        adopt: (id) => client.investigations.get(id),
        commit: (state, value) => {
          state.ids.investigation_id = value.id;
        },
        create: () => client.investigations.create(request),
        fingerprintInput: request,
        key: "investigation",
        kind: "investigation",
        manifest,
        parentIds: { agent_id: agentId, session_id: sessionId },
        reconcile: () => findInvestigationsByName(client, agentId, name),
        stage: "review",
        store,
        validate: (value) => {
          if (value.agent_id !== agentId || value.name !== name) {
            throw new Error(
              "Adopted investigation does not match the workflow",
            );
          }
        },
      },
      recovery,
    );
  } else {
    investigation = await client.investigations.get(
      manifest.ids.investigation_id,
    );
  }
  const linked = (
    await client.investigations.listSessions(investigation.id, { size: 20 })
  ).items;
  if (linked.length !== 1 || linked[0]?.session_id !== sessionId) {
    throw new Error("Investigation does not contain exactly ticket-004");
  }
  const investigationSession = linked[0];
  manifest.ids.investigation_session_id = investigationSession.id;
  await store.save(manifest);

  const answers = [
    {
      key: "outcome",
      selector: { node_id: refundNode.id, path: "/outputs" },
      value: {
        judgment: "problematic",
        reason: "The amount exceeds the automatic approval threshold.",
      },
    },
    {
      key: "expected",
      selector: null,
      value: { action: "escalate", reason: "Human approval is required." },
    },
    {
      key: "evidence",
      selector: { node_id: refundNode.id, path: "/outputs" },
      value: {
        node_id: refundNode.id,
        policy_condition: "refund amount exceeds human approval threshold",
      },
    },
  ];
  for (const [index, answer] of answers.entries()) {
    if (manifest.ids.annotation_ids.length > index) {
      continue;
    }
    const annotationRequest = {
      investigation_session_id: investigationSession.id,
      question_key: answer.key,
      selector: answer.selector,
      value: answer.value,
    };
    await runJournaledMutation<AnnotationResponse>(
      {
        adopt: (id) => client.annotations.get(id),
        commit: (state, value) => {
          state.ids.annotation_ids.push(value.id);
        },
        create: () => client.annotations.create(annotationRequest),
        fingerprintInput: annotationRequest,
        key: `annotation.${answer.key}`,
        kind: "annotation",
        manifest,
        parentIds: {
          investigation_id: investigation.id,
          investigation_session_id: investigationSession.id,
          session_id: sessionId,
        },
        stage: "review",
        store,
        validate: (value) => {
          if (value.session_id !== sessionId) {
            throw new Error("Adopted annotation belongs to another session");
          }
        },
      },
      recovery,
    );
  }
  await client.investigations.updateSession(investigation.id, sessionId, {
    verdict: "problematic",
  });
  await client.investigations.update(investigation.id, {
    status: "completed",
  });
  const settled = await client.investigations.get(investigation.id);
  if (
    settled.status !== "completed" ||
    settled.completed_sessions !== 1 ||
    settled.total_sessions !== 1
  ) {
    throw new Error(
      "Investigation did not settle exactly one reviewed session",
    );
  }
  setStage(manifest, "review", "completed");
  await store.save(manifest);
}

async function ensureEvaluator(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  evaluatorSource: string,
): Promise<{
  evaluator: EvaluatorResponse;
  version: EvaluatorVersionResponse;
}> {
  let blob: BlobResponse;
  if (manifest.ids.evaluator_blob_id === null) {
    blob = await runJournaledMutation(
      {
        adopt: (id) => client.blobs.get(id),
        commit: (state, value) => {
          state.ids.evaluator_blob_id = value.id;
        },
        create: () =>
          client.blobs.upload(new TextEncoder().encode(evaluatorSource), {
            filename: "returns_policy.py",
            mediaType: "text/x-python",
          }),
        fingerprintInput: {
          filename: "returns_policy.py",
          media_type: "text/x-python",
          sha256: manifest.source_hashes.evaluator_sha256,
        },
        key: "evaluator_blob",
        kind: "blob",
        manifest,
        stage: "baseline_evaluation",
        store,
        validate: (value) => {
          if (value.sha256 !== manifest.source_hashes.evaluator_sha256) {
            throw new Error(
              "Evaluator blob hash does not match documented source",
            );
          }
        },
      },
      recovery,
    );
  } else {
    blob = await client.blobs.get(manifest.ids.evaluator_blob_id);
  }
  const name = `returns-policy-${manifest.evidence_set_id.replaceAll("-", "").slice(0, 12)}`;
  let evaluator: EvaluatorResponse;
  if (manifest.ids.evaluator_id === null) {
    const request = {
      name,
      description: "README-derived synthetic returns policy.",
      metadata: {
        evidence_set_id: manifest.evidence_set_id,
        source_sha256: manifest.source_hashes.evaluator_sha256,
      },
    };
    evaluator = await runJournaledMutation(
      {
        adopt: (id) => client.evaluators.get(id),
        commit: (state, value) => {
          state.ids.evaluator_id = value.id;
        },
        create: () => client.evaluators.create(request),
        fingerprintInput: request,
        key: "evaluator",
        kind: "evaluator",
        manifest,
        reconcile: async () =>
          (await client.evaluators.list(createNamedFilter(name))).items,
        stage: "baseline_evaluation",
        store,
        validate: (value) => {
          if (value.name !== name) {
            throw new Error("Adopted evaluator does not match the workflow");
          }
        },
      },
      recovery,
    );
  } else {
    evaluator = await client.evaluators.get(manifest.ids.evaluator_id);
  }
  let version: EvaluatorVersionResponse;
  if (manifest.ids.evaluator_version_id === null) {
    const request = {
      display_version: "1.0",
      source: {
        blob_id: blob.id,
        entrypoint: "evaluate",
        type: "script" as const,
      },
    };
    version = await runJournaledMutation(
      {
        adopt: (id) => getEvaluatorVersionForAdoption(client, evaluator.id, id),
        commit: (state, value) => {
          state.ids.evaluator_version_id = value.id;
        },
        create: () => client.evaluators.createVersion(evaluator.id, request),
        fingerprintInput: request,
        key: "evaluator_version",
        kind: "evaluator_version",
        manifest,
        parentIds: { blob_id: blob.id, evaluator_id: evaluator.id },
        reconcile: async () =>
          (
            await client.evaluators.listVersions(evaluator.id, { size: 100 })
          ).items.filter(
            (candidate) =>
              candidate.display_version === "1.0" &&
              candidate.source.type === "script" &&
              candidate.source.blob_id === blob.id,
          ),
        stage: "baseline_evaluation",
        store,
        validate: (value) => {
          if (
            value.evaluator_id !== evaluator.id ||
            value.source.type !== "script" ||
            value.source.blob_id !== blob.id
          ) {
            throw new Error("Adopted evaluator version does not match source");
          }
        },
      },
      recovery,
    );
  } else {
    const versions = await client.evaluators.listVersions(evaluator.id, {
      size: 100,
    });
    const existing = versions.items.find(
      (candidate) => candidate.id === manifest.ids.evaluator_version_id,
    );
    if (existing === undefined) {
      throw new Error("Committed evaluator version no longer exists");
    }
    version = existing;
  }
  return { evaluator, version };
}

interface EvaluatorVersionAdoptionClient {
  evaluators: {
    iterVersions(
      evaluatorId: string,
      params?: { size?: number },
    ): AsyncIterable<EvaluatorVersionResponse>;
  };
}

export async function getEvaluatorVersionForAdoption(
  client: EvaluatorVersionAdoptionClient,
  evaluatorId: string,
  evaluatorVersionId: string,
): Promise<EvaluatorVersionResponse> {
  for await (const version of client.evaluators.iterVersions(evaluatorId, {
    size: 100,
  })) {
    if (version.id === evaluatorVersionId) {
      return version;
    }
  }
  throw new Error(
    `Evaluator version ${evaluatorVersionId} does not belong to evaluator ${evaluatorId}`,
  );
}

interface CohortVersionAdoptionClient {
  cohortVersions: {
    get(cohortVersionId: string): Promise<CohortVersionResponse>;
  };
}

export async function getCohortVersionForAdoption(
  client: CohortVersionAdoptionClient,
  cohortVersionId: string,
): Promise<CohortVersionResponse> {
  return client.cohortVersions.get(cohortVersionId);
}

async function collectTasks(
  client: KitaruClient,
  manifest: WorkflowManifest,
  jobIds: readonly string[],
): Promise<void> {
  const taskIds = new Set(manifest.ids.task_ids);
  const tasksByJob = await Promise.all(
    jobIds.map(async (jobId) => {
      const ids: string[] = [];
      for await (const task of client.jobs.iterTasks(jobId, { size: 100 })) {
        ids.push(task.id);
      }
      return ids;
    }),
  );
  for (const ids of tasksByJob) {
    for (const id of ids) taskIds.add(id);
  }
  manifest.ids.task_ids = [...taskIds].sort();
}

export async function verifyCompletedJob(
  job: JobResponse,
  kind: "evaluation" | "replay",
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  stage: "baseline_evaluation" | "experiment_runs",
): Promise<boolean> {
  if (!TERMINAL_JOB_STATUSES.has(job.status)) {
    return false;
  }
  if (job.status !== "completed" || job.kind !== kind) {
    setStage(manifest, stage, "failed");
    await store.save(manifest);
    throw new Error(
      `Expected completed ${kind} job ${job.id}; found ${job.kind}/${job.status}: ${job.error ?? "no error"}`,
    );
  }
  return true;
}

async function submitBaselineEvaluation(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  evaluator: EvaluatorResponse,
  evaluatorVersion: EvaluatorVersionResponse,
): Promise<JobResponse> {
  if (manifest.ids.evaluation_job_id !== null) {
    return client.jobs.get(manifest.ids.evaluation_job_id);
  }
  const inputSessionIds = Object.values(manifest.ids.baseline_sessions).sort();
  const request = {
    input_session_ids: inputSessionIds,
    ...workflowRequests.createEvaluator(
      evaluator.name,
      evaluatorVersion.version,
    ),
  };
  return runJournaledMutation(
    {
      adopt: (id) => client.jobs.get(id),
      commit: (state, value) => {
        state.ids.evaluation_job_id = value.id;
      },
      create: () => client.evaluations.create(request),
      fingerprintInput: request,
      key: "baseline_evaluation_job",
      kind: "evaluation_job",
      manifest,
      parentIds: { evaluator_version_id: evaluatorVersion.id },
      stage: "baseline_evaluation",
      store,
      validate: (value) => {
        if (
          value.kind !== "evaluation" ||
          value.owner_id !== manifest.server.account_id
        ) {
          throw new Error("Adopted job is not a baseline evaluation job");
        }
      },
    },
    recovery,
  );
}

async function listBaselineEvaluations(
  client: KitaruClient,
  manifest: WorkflowManifest,
): Promise<EvaluationResponse[]> {
  const evaluatorVersionId = assertId(
    manifest.ids.evaluator_version_id,
    "evaluator_version_id",
  );
  const page = await client.evaluations.list({
    filter: {
      field: "evaluator_version_id",
      op: "eq",
      value: evaluatorVersionId,
    },
    size: 100,
  });
  const baselineIds = new Set(Object.values(manifest.ids.baseline_sessions));
  return page.items.filter((evaluation) =>
    baselineIds.has(evaluation.session_id),
  );
}

function verifyBaselineResults(
  manifest: WorkflowManifest,
  evaluations: readonly EvaluationResponse[],
): void {
  if (evaluations.length !== COMPLETION_COUNTS.baseline_sessions) {
    throw new Error(
      `Expected ${COMPLETION_COUNTS.baseline_sessions} baseline evaluations; found ${evaluations.length}`,
    );
  }
  const ticketBySession = new Map(
    Object.entries(manifest.ids.baseline_sessions).map(([ticket, session]) => [
      session,
      ticket,
    ]),
  );
  const failures = evaluations
    .filter(({ passed }) => passed === false)
    .map(({ session_id }) => ticketBySession.get(session_id))
    .sort();
  if (
    JSON.stringify(failures) !==
      JSON.stringify([...BASELINE_FAILURE_TICKETS]) ||
    evaluations.filter(({ passed }) => passed === true).length !==
      COMPLETION_COUNTS.baseline_passes
  ) {
    throw new Error(
      `Baseline gate expected ${COMPLETION_COUNTS.baseline_passes} passes and failures only on ${BASELINE_FAILURE_TICKETS.join(
        ", ",
      )}; found failures ${failures.join(", ")}`,
    );
  }
}

async function ensureCohort(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  group: "target" | "control",
  tickets: readonly string[],
): Promise<CohortVersionResponse> {
  const agentId = assertId(manifest.ids.agent_id, "agent_id");
  const existingVersion = manifest.ids.cohort_versions[group];
  if (existingVersion !== null) {
    return client.cohortVersions.get(existingVersion);
  }
  const name = `${group === "target" ? "unsafe-refund-baseline" : "safe-refund-control"}-${manifest.evidence_set_id.replaceAll("-", "").slice(0, 12)}`;
  let cohort: CohortResponse;
  const cohortId = manifest.ids.cohorts[group];
  if (cohortId === null) {
    const request = {
      agent_id: agentId,
      name,
      metadata: { evidence_set_id: manifest.evidence_set_id, group },
    };
    cohort = await runJournaledMutation(
      {
        adopt: (id) => client.cohorts.get(id),
        commit: (state, value) => {
          state.ids.cohorts[group] = value.id;
        },
        create: () => client.cohorts.create(request),
        fingerprintInput: request,
        key: `cohort.${group}`,
        kind: "cohort",
        manifest,
        parentIds: { agent_id: agentId },
        reconcile: async () =>
          (await client.cohorts.list(createNamedFilter(name))).items,
        stage: "cohorts",
        store,
        validate: (value) => {
          if (value.agent_id !== agentId || value.name !== name) {
            throw new Error(`Adopted ${group} cohort does not match workflow`);
          }
        },
      },
      recovery,
    );
  } else {
    cohort = await client.cohorts.get(cohortId);
  }
  const sessionIds = tickets.map((ticket) => {
    const sessionId = manifest.ids.baseline_sessions[ticket];
    if (sessionId === undefined) {
      throw new Error(`Missing baseline session for ${ticket}`);
    }
    return sessionId;
  });
  const request = {
    add_session_ids: sessionIds,
    display_version:
      group === "target" ? "baseline-targets" : "baseline-controls",
  };
  return runJournaledMutation(
    {
      adopt: (id) => getCohortVersionForAdoption(client, id),
      commit: (state, value) => {
        state.ids.cohort_versions[group] = value.id;
      },
      create: () => client.cohorts.createVersion(cohort.id, request),
      fingerprintInput: request,
      key: `cohort_version.${group}`,
      kind: "cohort_version",
      manifest,
      parentIds: { cohort_id: cohort.id },
      reconcile: async () =>
        (
          await client.cohorts.listVersions(cohort.id, { size: 100 })
        ).items.filter(
          (version) => version.display_version === request.display_version,
        ),
      stage: "cohorts",
      store,
      validate: (value) => {
        if (
          value.cohort_id !== cohort.id ||
          value.session_count !== tickets.length
        ) {
          throw new Error(
            `Adopted ${group} cohort version does not match workflow`,
          );
        }
      },
    },
    recovery,
  );
}

async function ensureExperiment(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  evaluator: EvaluatorResponse,
  evaluatorVersion: EvaluatorVersionResponse,
): Promise<ExperimentResponse> {
  const existing = manifest.ids.experiment_id;
  if (existing !== null) {
    return client.experiments.get(existing);
  }
  const agentId = assertId(manifest.ids.agent_id, "agent_id");
  const name = `improve-returns-policy-${manifest.evidence_set_id.replaceAll("-", "").slice(0, 12)}`;
  const request = {
    agent_id: agentId,
    name,
    tool_policy: workflowRequests.toolPolicy,
    evaluators: [
      { evaluator: evaluator.name, version: evaluatorVersion.version },
    ],
  };
  return runJournaledMutation(
    {
      adopt: (id) => client.experiments.get(id),
      commit: (state, value) => {
        state.ids.experiment_id = value.id;
      },
      create: () => client.experiments.create(request),
      fingerprintInput: request,
      key: "experiment",
      kind: "experiment",
      manifest,
      parentIds: { agent_id: agentId },
      reconcile: async () =>
        (await client.experiments.list(createNamedFilter(name))).items,
      stage: "experiment_runs",
      store,
      validate: (value) => {
        if (value.agent_id !== agentId || value.name !== name) {
          throw new Error("Adopted experiment does not match workflow");
        }
      },
    },
    recovery,
  );
}

async function ensureExperimentRun(
  client: KitaruClient,
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  recovery: WorkflowArguments,
  group: "target" | "control",
): Promise<ExperimentRunResponse> {
  const existing = manifest.ids.experiment_run_ids[group];
  if (existing !== null) {
    return client.experimentRuns.get(existing);
  }
  const experimentId = assertId(manifest.ids.experiment_id, "experiment_id");
  const strictVersionId = assertId(
    manifest.ids.agent_versions.strict,
    "strict agent version",
  );
  const cohortVersionId = assertId(
    manifest.ids.cohort_versions[group],
    `${group} cohort version`,
  );
  const request = workflowRequests.createExperimentRun(
    strictVersionId,
    cohortVersionId,
  );
  return runJournaledMutation(
    {
      adopt: (id) => client.experimentRuns.get(id),
      commit: (state, value) => {
        state.ids.experiment_run_ids[group] = value.id;
      },
      create: () => client.experiments.startRun(experimentId, request),
      fingerprintInput: request,
      key: `experiment_run.${group}`,
      kind: "experiment_run",
      manifest,
      parentIds: {
        cohort_version_id: cohortVersionId,
        experiment_id: experimentId,
      },
      stage: "experiment_runs",
      store,
      validate: (value) => {
        if (
          value.experiment_id !== experimentId ||
          value.cohort_version_id !== cohortVersionId ||
          value.agent_version_id !== strictVersionId
        ) {
          throw new Error(`Adopted ${group} run does not match workflow`);
        }
      },
    },
    recovery,
  );
}

async function listReplaysForRun(
  client: KitaruClient,
  runId: string,
): Promise<ReplayResponse[]> {
  return (
    await client.replays.list({
      filter: { field: "experiment_run_id", op: "eq", value: runId },
      size: 100,
    })
  ).items;
}

async function emitBaselineHandoff(
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  stateDirectory: string,
): Promise<ReturnType<typeof createWorkerHandoffEvent>> {
  setStage(manifest, "baseline_evaluation", "awaiting_worker");
  await store.save(manifest);
  return createWorkerHandoffEvent({
    evidenceSetId: manifest.evidence_set_id,
    phase: "baseline_evaluation",
    stateDirectory,
    jobs: [
      {
        agent_version_id: null,
        job_id: assertId(manifest.ids.evaluation_job_id, "evaluation_job_id"),
        job_kind: "evaluation",
      },
    ],
  });
}

async function emitExperimentHandoff(
  manifest: WorkflowManifest,
  store: WorkflowManifestStore,
  stateDirectory: string,
): Promise<ReturnType<typeof createWorkerHandoffEvent>> {
  setStage(manifest, "experiment_runs", "awaiting_worker");
  await store.save(manifest);
  const strictVersion = assertId(
    manifest.ids.agent_versions.strict,
    "strict agent version",
  );
  const jobs: WorkerHandoffJob[] = manifest.ids.replay_job_ids.map((jobId) => ({
    agent_version_id: strictVersion,
    job_id: jobId,
    job_kind: "replay",
  }));
  return createWorkerHandoffEvent({
    evidenceSetId: manifest.evidence_set_id,
    jobs,
    phase: "experiment_runs",
    stateDirectory,
  });
}

async function initializeManifest(
  client: KitaruClient,
  apiUrl: string,
  store: WorkflowManifestStore,
  args: WorkflowArguments,
  hashes: WorkflowManifest["source_hashes"],
): Promise<WorkflowManifest> {
  let manifest = await store.load();
  if (manifest !== undefined && args.fresh) {
    await store.archive(manifest);
    manifest = undefined;
  }
  const [info, account] = await Promise.all([
    client.info.get(),
    client.accounts.getCurrent(),
  ]);
  if (manifest === undefined) {
    manifest = createWorkflowManifest({
      accountId: account.id,
      apiUrl,
      authScheme: info.auth_scheme,
      provider: args.provider,
      serverVersion: info.version,
      sourceHashes: hashes,
    });
    await store.save(manifest);
    return manifest;
  }
  if (
    manifest.server.api_url !== apiUrl ||
    manifest.server.account_id !== account.id ||
    manifest.server.auth_scheme !== info.auth_scheme ||
    manifest.server.version !== info.version
  ) {
    throw new Error("Workflow manifest belongs to another server or account");
  }
  if (
    manifest.provider.kind !== args.provider ||
    JSON.stringify(manifest.source_hashes) !== JSON.stringify(hashes)
  ) {
    throw new Error(
      "Workflow provider or source changed; pass --fresh to create new evidence",
    );
  }
  return manifest;
}

async function runWorkflowUnlocked(
  args: WorkflowArguments,
  environment: Readonly<Record<string, string | undefined>>,
  dependencies: WorkflowDependencies,
  store: WorkflowManifestStore,
) {
  validateWorkflowEnvironment(args.provider, environment);
  const material = await (
    dependencies.readSourceMaterial ?? readSourceMaterial
  )();
  const apiUrl = await resolveApiUrl(environment);
  const client = await (dependencies.createClient ?? createKitaruClient)({
    apiUrl,
    environment,
  });
  const manifest = await initializeManifest(
    client,
    apiUrl,
    store,
    args,
    material.hashes,
  );

  if (manifest.phase === "completed") {
    return createCompletedEvent(manifest.evidence_set_id, COMPLETION_COUNTS);
  }

  await ensureAgent(client, manifest, store, args);
  await ensureAgentVersion(client, manifest, store, args, "baseline");
  await recordBaselines(client, manifest, store, args, environment);
  await ensureReview(client, manifest, store, args);
  const { evaluator, version: evaluatorVersion } = await ensureEvaluator(
    client,
    manifest,
    store,
    args,
    material.evaluatorSource,
  );
  const evaluationJob = await submitBaselineEvaluation(
    client,
    manifest,
    store,
    args,
    evaluator,
    evaluatorVersion,
  );
  if (
    !(await verifyCompletedJob(
      evaluationJob,
      "evaluation",
      manifest,
      store,
      "baseline_evaluation",
    ))
  ) {
    return emitBaselineHandoff(manifest, store, args.stateDirectory);
  }
  await collectTasks(client, manifest, [evaluationJob.id]);
  const baselineResults = await listBaselineEvaluations(client, manifest);
  verifyBaselineResults(manifest, baselineResults);
  manifest.ids.evaluation_ids = baselineResults.map(({ id }) => id).sort();
  setStage(manifest, "baseline_evaluation", "completed");
  await store.save(manifest);

  await ensureCohort(client, manifest, store, args, "target", TARGET_TICKETS);
  await ensureCohort(client, manifest, store, args, "control", CONTROL_TICKETS);
  await ensureAgentVersion(client, manifest, store, args, "strict");
  setStage(manifest, "cohorts", "completed");
  await store.save(manifest);
  await ensureExperiment(
    client,
    manifest,
    store,
    args,
    evaluator,
    evaluatorVersion,
  );
  const targetRun = await ensureExperimentRun(
    client,
    manifest,
    store,
    args,
    "target",
  );
  const controlRun = await ensureExperimentRun(
    client,
    manifest,
    store,
    args,
    "control",
  );
  const [targetReplays, controlReplays] = await Promise.all([
    listReplaysForRun(client, targetRun.id),
    listReplaysForRun(client, controlRun.id),
  ]);
  const replayEntries = [...targetReplays, ...controlReplays];
  if (replayEntries.length !== COMPLETION_COUNTS.replays) {
    throw new Error(
      `Expected ${COMPLETION_COUNTS.replays} replays; found ${replayEntries.length}`,
    );
  }
  manifest.ids.replay_ids = replayEntries.map(({ id }) => id).sort();
  manifest.ids.replay_job_ids = replayEntries
    .map(({ job_id }) => job_id)
    .sort();
  await store.save(manifest);
  const replayJobs = await Promise.all(
    manifest.ids.replay_job_ids.map((id) => client.jobs.get(id)),
  );
  let replayJobsCompleted = true;
  for (const replayJob of replayJobs) {
    const completed = await verifyCompletedJob(
      replayJob,
      "replay",
      manifest,
      store,
      "experiment_runs",
    );
    replayJobsCompleted = replayJobsCompleted && completed;
  }
  if (!replayJobsCompleted) {
    return emitExperimentHandoff(manifest, store, args.stateDirectory);
  }

  await collectTasks(client, manifest, manifest.ids.replay_job_ids);
  const runs = await Promise.all([
    client.experimentRuns.get(targetRun.id),
    client.experimentRuns.get(controlRun.id),
  ]);
  for (const run of runs) {
    if (run.status !== "completed") {
      throw new Error(
        `Experiment run ${run.id} did not complete: ${run.status}`,
      );
    }
  }
  const replays = await Promise.all(
    manifest.ids.replay_ids.map((id) => client.replays.get(id)),
  );
  for (const replay of replays) {
    if (replay.status !== "completed" || replay.result_session_id == null) {
      throw new Error(`Replay ${replay.id} did not complete: ${replay.status}`);
    }
  }
  manifest.ids.replay_result_session_ids = replays
    .map(({ result_session_id }) =>
      assertId(result_session_id, "replay result session"),
    )
    .sort();
  const allEvaluations = (
    await client.evaluations.list({
      filter: {
        field: "evaluator_version_id",
        op: "eq",
        value: evaluatorVersion.id,
      },
      size: 100,
    })
  ).items;
  const resultIds = new Set(manifest.ids.replay_result_session_ids);
  const replayEvaluations = allEvaluations.filter(({ session_id }) =>
    resultIds.has(session_id),
  );
  if (
    replayEvaluations.length !== COMPLETION_COUNTS.replay_passes ||
    !replayEvaluations.every(({ passed }) => passed === true)
  ) {
    throw new Error(
      `Expected ${COMPLETION_COUNTS.replay_passes} passing replay evaluations`,
    );
  }
  manifest.ids.replay_evaluation_ids = replayEvaluations
    .map(({ id }) => id)
    .sort();
  setStage(manifest, "experiment_runs", "completed");
  setStage(manifest, "verification", "completed");
  manifest.phase = "completed";
  await store.save(manifest);
  return createCompletedEvent(manifest.evidence_set_id, COMPLETION_COUNTS);
}

export async function runWorkflow(
  args: WorkflowArguments,
  environment: Readonly<Record<string, string | undefined>> = process.env,
  dependencies: WorkflowDependencies = {},
) {
  const store = new WorkflowManifestStore(args.stateDirectory);
  const runLock = await store.acquireRunLock();
  try {
    return await runWorkflowUnlocked(args, environment, dependencies, store);
  } finally {
    await runLock.release();
  }
}

export function parseWorkflowArguments(
  args: readonly string[],
): WorkflowArguments {
  const parsed: WorkflowArguments = {
    adoptions: {},
    fresh: false,
    provider: "deterministic",
    retries: new Set(),
    stateDirectory: resolve(".state"),
  };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--fresh") {
      parsed.fresh = true;
      continue;
    }
    if (argument === "--provider") {
      const provider = args[index + 1];
      if (provider !== "deterministic" && provider !== "openai") {
        throw new Error("--provider must be deterministic or openai");
      }
      parsed.provider = provider;
      index += 1;
      continue;
    }
    if (argument === "--state-dir") {
      const directory = args[index + 1];
      if (!directory) {
        throw new Error("--state-dir must be followed by a directory");
      }
      parsed.stateDirectory = resolve(directory);
      index += 1;
      continue;
    }
    if (argument === "--adopt") {
      const value = args[index + 1];
      const separator = value?.indexOf("=") ?? -1;
      if (!value || separator < 1) {
        throw new Error("--adopt must be followed by operation=uuid");
      }
      parsed.adoptions[value.slice(0, separator)] = value.slice(separator + 1);
      index += 1;
      continue;
    }
    if (argument === "--retry") {
      const key = args[index + 1];
      if (!key) {
        throw new Error("--retry must be followed by an operation key");
      }
      parsed.retries.add(key);
      index += 1;
      continue;
    }
    throw new Error(`Unknown workflow argument: ${argument}`);
  }
  return parsed;
}

export async function main(): Promise<void> {
  try {
    const event = await runWorkflow(
      parseWorkflowArguments(process.argv.slice(2)),
    );
    process.stdout.write(`${JSON.stringify(event)}\n`);
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
