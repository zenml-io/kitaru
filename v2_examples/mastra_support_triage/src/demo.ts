import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type {
  AgentResponse,
  AgentVersionResponse,
  BlobResponse,
  EvaluationResponse,
  EvaluatorResponse,
  EvaluatorVersionResponse,
  JobResponse,
  KitaruClient,
  ReplayResponse,
  SessionNodeResponse,
} from "@zenml-io/kitaru";
import {
  createKitaruClient,
  readSelectedServerUrl,
  resolveConfigDirectory,
} from "@zenml-io/kitaru/node";

import {
  runOwnedJob,
  verifyOwnedJob,
  verifyReplaceableJob,
} from "./management.js";
import {
  AmbiguousOperationError,
  type CreateRemoteOptions,
  isUuid,
  type OperationKind,
  operationFingerprint,
  parseOperationKind,
  type RunManifest,
  RunManifestStore,
} from "./run-state.js";
import type { runDedicatedWorker } from "./worker.js";

const EXAMPLE_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const REPO_ROOT = resolve(EXAMPLE_DIR, "../..");
const SCORER_SOURCE = resolve(EXAMPLE_DIR, "scorers.py");
const RUN_COMMAND = "node v2_examples/mastra_support_triage/dist/main.js";
const REQUESTED_MODEL_ID = "openai/gpt-5-nano";
const INITIAL_PROMPT =
  "Investigate account acct-1001 and delayed order ord-1001. " +
  "The customer reports a suspected duplicate charge.";
const OVERRIDE_PROMPT =
  "Priority escalation: investigate account acct-1001 and order ord-1001. " +
  "Confirm the delayed order and suspected duplicate charge from tool evidence.";
const OVERRIDE_SYSTEM =
  "Follow the configured support workflow. Use the account and order lookup " +
  "tools and queue one refund review for a delayed duplicate charge. Answer " +
  "with a JSON object only, using exactly the keys decision, evidence, risk, " +
  "and nextAction, and record the queued refund review under evidence.";
const REPLACEABLE_OPERATIONS = new Set<OperationKind>([
  "create_initial_job",
  "create_replay",
]);

export interface DemoResult {
  evaluations: EvaluationResponse[];
  initial_nodes: SessionNodeResponse[];
  initial_outbox_count: number;
  initial_session_id: string;
  replay: ReplayResponse;
  replay_nodes: SessionNodeResponse[];
  replay_outbox_count: number;
  result_session_id: string;
  state_dir: string;
}

export interface RunDemoOptions {
  adoptions?: Partial<Record<OperationKind, string>>;
  apiUrl?: string;
  environment?: NodeJS.ProcessEnv;
  resumeStateDir?: string;
  runId?: string;
  retries?: ReadonlySet<OperationKind>;
  stateRoot?: string;
  testModel?: boolean;
  unauthenticated?: boolean;
}

export interface DemoDependencies {
  client?: KitaruClient;
  createClient?: typeof createKitaruClient;
  readScorer?: () => Promise<Uint8Array>;
  runWorker?: typeof runDedicatedWorker;
}

function normalizeServerUrl(value: string): string {
  const url = new URL(value);
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "Kitaru server URL must be an absolute HTTP(S) URL without credentials, query, or fragment",
    );
  }
  const path = url.pathname.replace(/\/+$/, "");
  return `${url.origin}${path}`;
}

async function resolveServerUrl(
  options: RunDemoOptions,
  environment: NodeJS.ProcessEnv,
): Promise<string> {
  const explicit = options.apiUrl ?? environment.KITARU_API_URL;
  if (explicit !== undefined) {
    return normalizeServerUrl(explicit);
  }
  return normalizeServerUrl(
    await readSelectedServerUrl(resolveConfigDirectory(environment)),
  );
}

function requireResource(
  manifest: RunManifest,
  key: keyof RunManifest["resources"],
): string {
  const value = manifest.resources[key];
  if (value === undefined) {
    throw new Error(`Run manifest is missing ${key}`);
  }
  return value;
}

async function saveStatus(
  store: RunManifestStore,
  status: RunManifest["status"],
): Promise<RunManifest> {
  const manifest = await store.read();
  manifest.status = status;
  await store.save(manifest);
  return manifest;
}

async function countOutbox(stateDir: string): Promise<number> {
  try {
    return (
      await readFile(resolve(stateDir, "refund-review-outbox.jsonl"), "utf8")
    )
      .split(/\r?\n/)
      .filter(Boolean).length;
  } catch (error) {
    if (
      error instanceof Error &&
      "code" in error &&
      (error as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return 0;
    }
    throw error;
  }
}

function requireNonemptyText(outputs: unknown): string {
  if (
    typeof outputs !== "object" ||
    outputs === null ||
    !("text" in outputs) ||
    typeof outputs.text !== "string" ||
    outputs.text.trim().length === 0
  ) {
    throw new Error(
      "The baseline session did not record non-empty output text",
    );
  }
  return outputs.text;
}

function assertRecordedShape(nodes: readonly SessionNodeResponse[]): void {
  const roots = nodes.filter(
    (node) => node.node_type === "span" && node.parent_index === null,
  );
  const llmNodes = nodes.filter((node) => node.node_type === "llm_call");
  const toolNames = new Set(
    nodes
      .filter((node) => node.node_type === "tool_call")
      .map((node) => node.tool_name),
  );
  if (
    roots.length !== 1 ||
    llmNodes.length < 2 ||
    !["lookupAccount", "lookupOrder", "queueRefundReview"].every((name) =>
      toolNames.has(name),
    )
  ) {
    throw new Error("The recorded session is missing the expected trace shape");
  }
}

async function getResultSessionId(
  client: KitaruClient,
  job: JobResponse,
): Promise<string> {
  const tasks = await client.jobs.listTasks(job.id);
  if (tasks.next_cursor !== null) {
    throw new Error(`Job ${job.id} has too many tasks to inspect safely`);
  }
  const ids = tasks.items.flatMap((task) =>
    task.kind === "agent" && task.result_session_id !== null
      ? [task.result_session_id]
      : [],
  );
  if (ids.length !== 1 || ids[0] === undefined) {
    throw new Error(
      `Kitaru job ${job.id} produced ${ids.length} result sessions`,
    );
  }
  return ids[0];
}

async function listSessionNodes(
  client: KitaruClient,
  sessionId: string,
): Promise<SessionNodeResponse[]> {
  const result: SessionNodeResponse[] = [];
  for await (const node of client.sessions.iterNodes(sessionId, {
    includePayloads: true,
  })) {
    result.push(node);
  }
  return result;
}

async function listEvaluationsForSession(
  client: KitaruClient,
  sessionId: string,
): Promise<EvaluationResponse[]> {
  const evaluations: EvaluationResponse[] = [];
  for await (const evaluation of client.evaluations.iter({
    filter: { field: "session_id", op: "eq", value: sessionId },
  })) {
    evaluations.push(evaluation);
  }
  return evaluations;
}

async function collectResult(
  client: KitaruClient,
  store: RunManifestStore,
): Promise<DemoResult> {
  const manifest = await store.read();
  const initialSessionId = requireResource(manifest, "initial_session_id");
  const resultSessionId = requireResource(manifest, "result_session_id");
  const replay = await client.replays.get(
    requireResource(manifest, "replay_id"),
  );
  if (
    replay.job_id !== requireResource(manifest, "replay_job_id") ||
    replay.baseline_session_id !== initialSessionId ||
    replay.result_session_id !== resultSessionId ||
    replay.status !== "completed"
  ) {
    throw new Error(
      "The completed replay no longer matches the durable run manifest",
    );
  }
  if (manifest.summary === undefined) {
    throw new Error("Completed run manifest is missing its summary");
  }
  const [initialNodes, replayNodes, evaluations] = await Promise.all([
    listSessionNodes(client, initialSessionId),
    listSessionNodes(client, resultSessionId),
    listEvaluationsForSession(client, resultSessionId),
  ]);
  return {
    evaluations,
    initial_nodes: initialNodes,
    initial_outbox_count: manifest.summary.initial_outbox_count,
    initial_session_id: initialSessionId,
    replay,
    replay_nodes: replayNodes,
    replay_outbox_count: manifest.summary.replay_outbox_count,
    result_session_id: resultSessionId,
    state_dir: store.stateDir,
  };
}

async function createStore(
  options: RunDemoOptions,
  serverUrl: string,
  ownerId: string,
): Promise<RunManifestStore> {
  return RunManifestStore.create({
    ownerId,
    rootDir: resolve(options.stateRoot ?? resolve(EXAMPLE_DIR, ".state")),
    runId: options.runId,
    serverUrl,
  });
}

function assertManifestIdentity(
  manifest: RunManifest,
  serverUrl: string,
  ownerId: string,
  options: RunDemoOptions,
): void {
  if (manifest.server_url !== serverUrl || manifest.owner_id !== ownerId) {
    throw new Error(
      "Run manifest belongs to a different Kitaru server or account",
    );
  }
  const unresolved = manifest.operations.find(
    (operation) =>
      operation.state !== "committed" && operation.state !== "retried",
  );
  const requestedKinds = new Set<OperationKind>([
    ...Object.keys(options.adoptions ?? {}).map(parseOperationKind),
    ...(options.retries ?? []),
  ]);
  if (unresolved === undefined) {
    if (requestedKinds.size === 0) return;
    const requestedKind = [...requestedKinds][0];
    if (
      requestedKinds.size === 1 &&
      requestedKind !== undefined &&
      REPLACEABLE_OPERATIONS.has(requestedKind) &&
      options.retries?.has(requestedKind) === true &&
      options.adoptions?.[requestedKind] === undefined
    ) {
      return;
    }
    const retryAuthorization = manifest.operations.at(-1);
    if (
      requestedKinds.size !== 1 ||
      retryAuthorization?.state !== "retried" ||
      !requestedKinds.has(retryAuthorization.kind) ||
      options.retries?.has(retryAuthorization.kind) !== true ||
      options.adoptions?.[retryAuthorization.kind] !== undefined
    ) {
      throw new Error("This run has no matching operation to recover");
    }
    return;
  }
  if (
    requestedKinds.size !== 1 ||
    !requestedKinds.has(unresolved.kind) ||
    (options.adoptions?.[unresolved.kind] !== undefined &&
      options.retries?.has(unresolved.kind) === true)
  ) {
    throw new AmbiguousOperationError(unresolved);
  }
}

function createRecoveryOptions<T>(
  options: RunDemoOptions,
  kind: OperationKind,
  adopt: (remoteId: string) => Promise<T>,
  validateAdopted: (remote: T) => Promise<void> | void,
  commit: (manifest: RunManifest, remote: T) => void,
): CreateRemoteOptions<T> {
  return {
    adopt,
    adoptionId: options.adoptions?.[kind],
    commit,
    retry: options.retries?.has(kind),
    validateAdopted,
  };
}

async function getEvaluatorVersionById(
  client: KitaruClient,
  evaluatorId: string,
  versionId: string,
): Promise<EvaluatorVersionResponse> {
  for await (const version of client.evaluators.iterVersions(evaluatorId)) {
    if (version.id === versionId) return version;
  }
  throw new Error(
    `Evaluator ${evaluatorId} does not contain version ${versionId}`,
  );
}

export async function runDemo(
  options: RunDemoOptions = {},
  dependencies: DemoDependencies = {},
): Promise<DemoResult> {
  const environment = options.environment ?? process.env;
  if (!options.testModel && !environment.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is required");
  }
  const serverUrl = await resolveServerUrl(options, environment);
  const createClient = dependencies.createClient ?? createKitaruClient;
  const client =
    dependencies.client ??
    (await createClient({
      apiUrl: serverUrl,
      ...(options.unauthenticated
        ? { credentialProvider: async () => undefined }
        : {}),
      environment,
    }));
  if (options.resumeStateDir !== undefined) {
    const store = RunManifestStore.open(options.resumeStateDir);
    return store.withRunLock(async () => {
      const account = await client.accounts.getCurrent();
      return runLockedDemo(
        options,
        dependencies,
        serverUrl,
        client,
        account,
        store,
      );
    });
  }
  const account = await client.accounts.getCurrent();
  const store = await createStore(options, serverUrl, account.id);
  return store.withRunLock(() =>
    runLockedDemo(options, dependencies, serverUrl, client, account, store),
  );
}

async function runLockedDemo(
  options: RunDemoOptions,
  dependencies: DemoDependencies,
  serverUrl: string,
  client: KitaruClient,
  account: Awaited<ReturnType<KitaruClient["accounts"]["getCurrent"]>>,
  store: RunManifestStore,
): Promise<DemoResult> {
  let manifest = await store.read();
  assertManifestIdentity(manifest, serverUrl, account.id, options);
  if (manifest.status === "completed") {
    return collectResult(client, store);
  }

  const suffix = manifest.run_id.replaceAll("-", "").slice(0, 10);
  if (manifest.resources.agent_id === undefined) {
    const request = {
      description: "Mastra record/replay demo.",
      name: `mastra-support-triage-${suffix}`,
    };
    await store.createRemote(
      "create_agent",
      operationFingerprint(request),
      () => client.agents.create(request),
      createRecoveryOptions<AgentResponse>(
        options,
        "create_agent",
        (id) => client.agents.get(id),
        (agent) => {
          if (agent.owner_id !== account.id || agent.name !== request.name) {
            throw new Error("Adopted agent does not match this run");
          }
        },
        (current, agent) => {
          current.resources.agent_id = agent.id;
        },
      ),
    );
  }
  manifest = await store.read();
  const agentId = requireResource(manifest, "agent_id");
  const runEnvironment: Record<string, string> = {
    KITARU_AGENT_ID: agentId,
    KITARU_SUPPORT_TRIAGE_STATE_DIR: store.stateDir,
  };
  if (options.testModel) {
    runEnvironment.KITARU_MASTRA_TEST_MODEL = "1";
  }
  if (manifest.resources.agent_version_id === undefined) {
    const request = {
      capabilities: {
        tools: ["lookupAccount", "lookupOrder", "queueRefundReview"],
      },
      description: "OpenAI gpt-5-nano support triage.",
      display_version: "v1",
      run_spec: {
        command: RUN_COMMAND,
        env: runEnvironment,
        timeout_seconds: 120,
        working_dir: REPO_ROOT,
      },
    };
    await store.createRemote(
      "create_agent_version",
      operationFingerprint({ agentId, request }),
      () => client.agents.createVersion(agentId, request),
      createRecoveryOptions<AgentVersionResponse>(
        options,
        "create_agent_version",
        (id) => client.agents.getVersion(id),
        (version) => {
          if (
            version.owner_id !== account.id ||
            version.agent_id !== agentId ||
            version.display_version !== request.display_version
          ) {
            throw new Error("Adopted agent version does not match this run");
          }
        },
        (current, version) => {
          current.resources.agent_version_id = version.id;
        },
      ),
    );
  }
  manifest = await store.read();
  if (manifest.resources.evaluator_id === undefined) {
    const request = {
      description: "Deterministic checks for the Mastra demo.",
      name: `mastra-support-triage-${suffix}`,
    };
    await store.createRemote(
      "create_evaluator",
      operationFingerprint(request),
      () => client.evaluators.create(request),
      createRecoveryOptions<EvaluatorResponse>(
        options,
        "create_evaluator",
        (id) => client.evaluators.get(id),
        (evaluator) => {
          if (
            evaluator.owner_id !== account.id ||
            evaluator.name !== request.name
          ) {
            throw new Error("Adopted evaluator does not match this run");
          }
        },
        (current, evaluator) => {
          current.resources.evaluator_id = evaluator.id;
        },
      ),
    );
  }
  manifest = await store.read();
  if (manifest.resources.evaluator_blob_id === undefined) {
    const scorer = await (
      dependencies.readScorer ?? (() => readFile(SCORER_SOURCE))
    )();
    const scorerSha256 = createHash("sha256").update(scorer).digest("hex");
    await store.createRemote(
      "upload_evaluator_source",
      operationFingerprint({
        content_sha256: scorerSha256,
        filename: "scorers.py",
      }),
      () =>
        client.blobs.upload(scorer, {
          filename: "scorers.py",
          mediaType: "text/x-python",
        }),
      createRecoveryOptions<BlobResponse>(
        options,
        "upload_evaluator_source",
        (id) => client.blobs.get(id),
        (blob) => {
          if (
            blob.sha256 !== scorerSha256 ||
            blob.size !== scorer.byteLength ||
            blob.media_type !== "text/x-python"
          ) {
            throw new Error("Adopted evaluator source does not match this run");
          }
        },
        (current, blob) => {
          current.resources.evaluator_blob_id = blob.id;
        },
      ),
    );
  }
  manifest = await store.read();
  const evaluatorId = requireResource(manifest, "evaluator_id");
  if (manifest.resources.evaluator_version_id === undefined) {
    const request = {
      display_version: "v1",
      source: {
        blob_id: requireResource(manifest, "evaluator_blob_id"),
        entrypoint: "evaluate",
        type: "script" as const,
      },
    };
    await store.createRemote(
      "create_evaluator_version",
      operationFingerprint({ evaluatorId, request }),
      () => client.evaluators.createVersion(evaluatorId, request),
      createRecoveryOptions<EvaluatorVersionResponse>(
        options,
        "create_evaluator_version",
        (id) => getEvaluatorVersionById(client, evaluatorId, id),
        (version) => {
          if (
            version.evaluator_id !== evaluatorId ||
            version.display_version !== request.display_version ||
            version.source.type !== "script" ||
            version.source.blob_id !== request.source.blob_id ||
            version.source.entrypoint !== request.source.entrypoint
          ) {
            throw new Error(
              "Adopted evaluator version does not match this run",
            );
          }
        },
        (current, version) => {
          current.resources.evaluator_version_id = version.id;
        },
      ),
    );
  }
  manifest = await saveStatus(store, "recording");
  const agentVersionId = requireResource(manifest, "agent_version_id");
  const initialRequest = {
    agent_version_id: agentVersionId,
    inputs: INITIAL_PROMPT,
    name: "Mastra support triage baseline",
  };
  const initialFingerprint = operationFingerprint(initialRequest);
  const replaceInitialJob = options.retries?.has("create_initial_job") === true;
  if (manifest.resources.initial_job_id === undefined || replaceInitialJob) {
    if (manifest.resources.initial_job_id !== undefined) {
      const oldJobId = manifest.resources.initial_job_id;
      await verifyReplaceableJob({
        client,
        expectedAgentVersionId: agentVersionId,
        expectedKind: "session_run",
        jobId: oldJobId,
        ownerId: account.id,
      });
      await store.authorizeReplacement(
        "create_initial_job",
        initialFingerprint,
        [oldJobId],
      );
    }
    await store.createRemote(
      "create_initial_job",
      initialFingerprint,
      () => client.sessionRuns.create(initialRequest),
      createRecoveryOptions<JobResponse>(
        options,
        "create_initial_job",
        (id) => client.jobs.get(id),
        (job) =>
          verifyOwnedJob({
            client,
            expectedAgentVersionId: agentVersionId,
            expectedKind: "session_run",
            jobId: job.id,
            observedJob: job,
            ownerId: account.id,
          }).then(() => undefined),
        (current, job) => {
          current.resources.initial_job_id = job.id;
        },
      ),
    );
  }
  manifest = await store.read();
  const initialJobId = requireResource(manifest, "initial_job_id");
  const initialJob = await runOwnedJob({
    apiUrl: serverUrl,
    client,
    expectedAgentVersionId: agentVersionId,
    expectedKind: "session_run",
    jobId: initialJobId,
    ownerId: account.id,
    runWorker: dependencies.runWorker,
    store,
  });
  let initialSessionId = manifest.resources.initial_session_id;
  if (initialSessionId === undefined) {
    initialSessionId = await getResultSessionId(client, initialJob);
    manifest = await store.read();
    manifest.resources.initial_session_id = initialSessionId;
    await store.save(manifest);
  }
  const [initialSession, initialNodes, initialOutboxCount] = await Promise.all([
    client.sessions.get(initialSessionId),
    listSessionNodes(client, initialSessionId),
    countOutbox(store.stateDir),
  ]);
  if (
    initialSession.owner_id !== account.id ||
    initialSession.status !== "completed"
  ) {
    throw new Error(
      `Baseline session ${initialSessionId} is not a completed owned session`,
    );
  }
  requireNonemptyText(initialSession.outputs);
  assertRecordedShape(initialNodes);
  if (initialOutboxCount !== 1) {
    throw new Error(
      `Baseline run wrote ${initialOutboxCount} outbox lines, expected exactly one`,
    );
  }

  manifest = await saveStatus(store, "replaying");
  const replayRequest = {
    agent_version_id: agentVersionId,
    baseline_session_id: initialSessionId,
    evaluate_baselines: false,
    evaluators: [{ evaluator: `mastra-support-triage-${suffix}` }],
    override: {
      model_params: { maxOutputTokens: 3000 },
      prompt: OVERRIDE_PROMPT,
      system_prompt: OVERRIDE_SYSTEM,
    },
    tool_policy: {
      default: { type: "passthrough" as const },
      tools: {
        queueRefundReview: {
          on_miss: "fail" as const,
          scope: "baseline" as const,
          type: "history" as const,
        },
      },
    },
  };
  const replayFingerprint = operationFingerprint(replayRequest);
  const replaceReplay = options.retries?.has("create_replay") === true;
  if (manifest.resources.replay_id === undefined || replaceReplay) {
    if (manifest.resources.replay_id !== undefined) {
      const oldReplayId = manifest.resources.replay_id;
      const oldReplayJobId = requireResource(manifest, "replay_job_id");
      const oldReplay = await client.replays.get(oldReplayId);
      if (
        oldReplay.job_id !== oldReplayJobId ||
        oldReplay.baseline_session_id !== initialSessionId
      ) {
        throw new Error(
          `Replay ${oldReplayId} does not match this run manifest`,
        );
      }
      await verifyReplaceableJob({
        client,
        expectedAgentVersionId: agentVersionId,
        expectedKind: "replay",
        jobId: oldReplayJobId,
        ownerId: account.id,
      });
      await store.authorizeReplacement("create_replay", replayFingerprint, [
        oldReplayId,
        oldReplayJobId,
      ]);
    }
    await store.createRemote(
      "create_replay",
      replayFingerprint,
      () => client.replays.create(replayRequest),
      createRecoveryOptions<ReplayResponse>(
        options,
        "create_replay",
        (id) => client.replays.get(id),
        async (replay) => {
          if (
            replay.baseline_session_id !== initialSessionId ||
            replay.evaluate_baselines !== replayRequest.evaluate_baselines
          ) {
            throw new Error("Adopted replay does not match this run");
          }
          await verifyOwnedJob({
            client,
            expectedAgentVersionId: agentVersionId,
            expectedKind: "replay",
            jobId: replay.job_id,
            ownerId: account.id,
          });
        },
        (current, replay) => {
          current.resources.replay_id = replay.id;
          current.resources.replay_job_id = replay.job_id;
        },
      ),
    );
  }
  manifest = await store.read();
  const replayId = requireResource(manifest, "replay_id");
  const replayBeforeRun = await client.replays.get(replayId);
  if (
    replayBeforeRun.job_id !== requireResource(manifest, "replay_job_id") ||
    replayBeforeRun.baseline_session_id !== initialSessionId
  ) {
    throw new Error(`Replay ${replayId} does not match this run manifest`);
  }
  await runOwnedJob({
    apiUrl: serverUrl,
    client,
    expectedAgentVersionId: agentVersionId,
    expectedKind: "replay",
    jobId: replayBeforeRun.job_id,
    ownerId: account.id,
    runWorker: dependencies.runWorker,
    store,
  });
  const replay = await client.replays.get(replayId);
  const replayResultSessionId = replay.result_session_id;
  if (replay.status !== "completed" || replayResultSessionId == null) {
    throw new Error(
      `Replay ${replayId} did not produce a completed result session`,
    );
  }
  const [replayOutboxCount, resultSession, replayNodes, evaluations] =
    await Promise.all([
      countOutbox(store.stateDir),
      client.sessions.get(replayResultSessionId),
      listSessionNodes(client, replayResultSessionId),
      listEvaluationsForSession(client, replayResultSessionId),
    ]);
  if (replayOutboxCount !== 1) {
    throw new Error(
      `History replay wrote ${replayOutboxCount} outbox lines, expected one`,
    );
  }
  if (
    resultSession.owner_id !== account.id ||
    resultSession.status !== "completed" ||
    resultSession.origin !== "replay" ||
    JSON.stringify(resultSession.inputs) !==
      JSON.stringify({
        prompt: OVERRIDE_PROMPT,
        system_prompt: OVERRIDE_SYSTEM,
      })
  ) {
    throw new Error(
      "Replay result session does not match the requested override",
    );
  }
  assertRecordedShape(replayNodes);
  const replayAction = replayNodes.filter(
    (node) =>
      node.node_type === "tool_call" && node.tool_name === "queueRefundReview",
  );
  const actionAttributes = replayAction[0]?.attributes;
  if (
    replayAction.length !== 1 ||
    typeof actionAttributes !== "object" ||
    actionAttributes === null ||
    !("mocked" in actionAttributes) ||
    actionAttributes.mocked !== true ||
    !("policy" in actionAttributes) ||
    actionAttributes.policy !== "history"
  ) {
    throw new Error("Replay did not safely reuse the recorded side effect");
  }
  const llmNodes = replayNodes.filter((node) => node.node_type === "llm_call");
  if (
    llmNodes.some(
      (node) =>
        node.requested_model !== REQUESTED_MODEL_ID ||
        node.model === REQUESTED_MODEL_ID ||
        node.cost == null ||
        !Number.isFinite(Number(node.cost)) ||
        Number(node.cost) <= 0,
    ) ||
    !llmNodes.some((node) => node.model_params?.maxOutputTokens === 3000)
  ) {
    throw new Error("Replay LLM nodes do not preserve model and cost evidence");
  }
  const expectedEvaluations = new Set([
    "decision_structure",
    "side_effect_safety",
    "trace_completeness",
  ]);
  if (
    evaluations.length !== expectedEvaluations.size ||
    evaluations.some(
      (evaluation) =>
        !expectedEvaluations.has(evaluation.name) || evaluation.passed !== true,
    )
  ) {
    throw new Error("Replay evaluations did not all pass");
  }

  manifest = await store.read();
  manifest.resources.result_session_id = replayResultSessionId;
  manifest.status = "completed";
  manifest.summary = {
    initial_outbox_count: initialOutboxCount,
    replay_outbox_count: replayOutboxCount,
  };
  await store.save(manifest);
  return {
    evaluations,
    initial_nodes: initialNodes,
    initial_outbox_count: initialOutboxCount,
    initial_session_id: initialSessionId,
    replay,
    replay_nodes: replayNodes,
    replay_outbox_count: replayOutboxCount,
    result_session_id: replayResultSessionId,
    state_dir: store.stateDir,
  };
}

interface CliArguments {
  adoptions: Partial<Record<OperationKind, string>>;
  apiUrl?: string;
  retries: Set<OperationKind>;
  resumeStateDir?: string;
  stateRoot?: string;
  testModel: boolean;
  unauthenticated: boolean;
}

export function parseArguments(args: readonly string[]): CliArguments {
  const parsed: CliArguments = {
    adoptions: {},
    retries: new Set(),
    testModel: false,
    unauthenticated: false,
  };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--") {
      continue;
    }
    if (argument === "--test-model") {
      parsed.testModel = true;
    } else if (argument === "--unauthenticated") {
      parsed.unauthenticated = true;
    } else if (
      argument === "--api-url" ||
      argument === "--adopt" ||
      argument === "--resume" ||
      argument === "--retry" ||
      argument === "--state-root"
    ) {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(`${argument} requires a value`);
      }
      index += 1;
      if (argument === "--api-url") parsed.apiUrl = value;
      if (argument === "--adopt") {
        const separator = value.indexOf("=");
        if (separator <= 0 || value.indexOf("=", separator + 1) !== -1) {
          throw new Error("--adopt requires operation=UUID");
        }
        const kind = parseOperationKind(value.slice(0, separator));
        const remoteId = value.slice(separator + 1);
        if (!isUuid(remoteId)) {
          throw new Error("--adopt requires operation=UUID");
        }
        if (parsed.adoptions[kind] !== undefined || parsed.retries.has(kind)) {
          throw new Error(`Recovery for ${kind} was provided more than once`);
        }
        parsed.adoptions[kind] = remoteId;
      }
      if (argument === "--resume") parsed.resumeStateDir = value;
      if (argument === "--retry") {
        const kind = parseOperationKind(value);
        if (parsed.retries.has(kind) || parsed.adoptions[kind] !== undefined) {
          throw new Error(`Recovery for ${kind} was provided more than once`);
        }
        parsed.retries.add(kind);
      }
      if (argument === "--state-root") parsed.stateRoot = value;
    } else {
      throw new Error(`Unknown argument: ${argument}`);
    }
  }
  return parsed;
}

function printResult(result: DemoResult): void {
  const action = result.replay_nodes.find(
    (node) =>
      node.node_type === "tool_call" && node.tool_name === "queueRefundReview",
  );
  const attributes =
    typeof action?.attributes === "object" && action.attributes !== null
      ? action.attributes
      : {};
  console.log(`state_dir=${result.state_dir}`);
  console.log(`initial_session_id=${result.initial_session_id}`);
  console.log(`replay_id=${result.replay.id}`);
  console.log(`result_session_id=${result.result_session_id}`);
  console.log(`outbox_after_record=${result.initial_outbox_count}`);
  console.log(`outbox_after_history_replay=${result.replay_outbox_count}`);
  console.log(
    `history_action=mocked:${String("mocked" in attributes ? attributes.mocked : undefined)},policy:${String("policy" in attributes ? attributes.policy : undefined)}`,
  );
  console.log(
    `evaluations=${result.evaluations.map((evaluation) => `${evaluation.name}:${String(evaluation.score)}`).join(",")}`,
  );
  console.log(`KITARU_DEMO_RESULT ${JSON.stringify(result)}`);
}

async function main(): Promise<void> {
  const arguments_ = parseArguments(process.argv.slice(2));
  printResult(await runDemo(arguments_));
}

if (
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await main();
}
