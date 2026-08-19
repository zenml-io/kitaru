import {
  type CredentialProvider,
  createStaticCredentialProvider,
} from "./auth/index.js";
import type {
  KitaruEnvironmentOptions,
  KitaruEnvironmentVariables,
} from "./environment.js";
import { resolveKitaruEnvironment } from "./environment.js";
import { KitaruApiError } from "./errors.js";
import {
  AccountsResource,
  AgentsResource,
  AnnotationsResource,
  BlobsResource,
  CohortsResource,
  CohortVersionsResource,
  EvaluationsResource,
  EvaluatorsResource,
  ExperimentRunsResource,
  ExperimentsResource,
  InfoResource,
  InvestigationsResource,
  JobsResource,
  ReplaysResource,
  type ResourceRequestOptions,
  SessionRunsResource,
  SessionsResource,
  TasksResource,
} from "./resources/index.js";
import { KitaruTransport } from "./transport.js";
import type {
  ReplayResponse,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionNodeResponse,
  SessionResponse,
  SessionUpdateRequest,
  TaskSpecResponse,
  ToolLookupRequest,
  ToolLookupResponse,
} from "./types.js";

export type {
  ReplayResponse,
  SessionCreateRequest,
  SessionNodeBatchRequest,
  SessionNodeResponse,
  SessionResponse,
  SessionUpdateRequest,
  TaskSpecResponse,
  ToolLookupRequest,
  ToolLookupResponse,
} from "./types.js";

export interface KitaruClientOptions extends KitaruEnvironmentOptions {
  credentialProvider?: CredentialProvider;
  environment?: KitaruEnvironmentVariables;
  fetch?: typeof globalThis.fetch;
}

export interface KitaruRequestOptions extends ResourceRequestOptions {}

export class KitaruClient {
  readonly #transport: KitaruTransport;
  readonly accounts: AccountsResource;
  readonly agents: AgentsResource;
  readonly annotations: AnnotationsResource;
  readonly blobs: BlobsResource;
  readonly cohortVersions: CohortVersionsResource;
  readonly cohorts: CohortsResource;
  readonly evaluations: EvaluationsResource;
  readonly evaluators: EvaluatorsResource;
  readonly experimentRuns: ExperimentRunsResource;
  readonly experiments: ExperimentsResource;
  readonly info: InfoResource;
  readonly investigations: InvestigationsResource;
  readonly jobs: JobsResource;
  readonly replays: ReplaysResource;
  readonly sessionRuns: SessionRunsResource;
  readonly sessions: SessionsResource;
  readonly tasks: TasksResource;

  constructor(options: KitaruClientOptions = {}) {
    const environment = resolveKitaruEnvironment(options, options.environment);
    const credentialProvider =
      options.apiKey !== undefined
        ? createStaticCredentialProvider(options.apiKey, "explicit")
        : (options.credentialProvider ??
          (environment.apiKey === undefined
            ? undefined
            : createStaticCredentialProvider(
                environment.apiKey,
                "environment",
              )));
    this.#transport = new KitaruTransport({
      apiUrl: environment.apiUrl,
      credentialProvider,
      fetch: options.fetch,
      timeoutMs: environment.timeoutMs,
    });
    this.accounts = new AccountsResource(this.#transport);
    this.agents = new AgentsResource(this.#transport);
    this.annotations = new AnnotationsResource(this.#transport);
    this.blobs = new BlobsResource(this.#transport);
    this.cohortVersions = new CohortVersionsResource(this.#transport);
    this.cohorts = new CohortsResource(this.#transport);
    this.evaluations = new EvaluationsResource(this.#transport);
    this.evaluators = new EvaluatorsResource(this.#transport);
    this.experimentRuns = new ExperimentRunsResource(this.#transport);
    this.experiments = new ExperimentsResource(this.#transport);
    this.info = new InfoResource(this.#transport);
    this.investigations = new InvestigationsResource(this.#transport);
    this.jobs = new JobsResource(this.#transport);
    this.replays = new ReplaysResource(this.#transport);
    this.sessionRuns = new SessionRunsResource(this.#transport);
    this.sessions = new SessionsResource(this.#transport);
    this.tasks = new TasksResource(this.#transport);
  }

  async createSession(
    request: SessionCreateRequest,
    options: KitaruRequestOptions = {},
  ): Promise<SessionResponse> {
    return this.sessions.create(request, options);
  }

  /**
   * Create a session, recovering from a task's already-linked result session.
   *
   * A retry of a task's session create can 409 when the first attempt
   * committed and linked the task's result session but its response was
   * lost. When taskId is set, that 409 is resolved by reading the task's
   * result session instead of failing the retry.
   */
  async createOrGetResultSession(
    request: SessionCreateRequest,
    taskId?: string,
    options: KitaruRequestOptions = {},
  ): Promise<SessionResponse> {
    try {
      return await this.sessions.create(request, options);
    } catch (error) {
      if (
        taskId === undefined ||
        !(error instanceof KitaruApiError) ||
        error.status !== 409
      ) {
        throw error;
      }
      const task = await this.tasks.get(taskId, options);
      if (
        task.result_session_id === undefined ||
        task.result_session_id === null
      ) {
        throw error;
      }
      return this.sessions.get(task.result_session_id, options);
    }
  }

  async updateSession(
    sessionId: string,
    request: SessionUpdateRequest,
    options: KitaruRequestOptions = {},
  ): Promise<SessionResponse> {
    return this.sessions.update(sessionId, request, options);
  }

  async upsertSessionNodes(
    sessionId: string,
    request: SessionNodeBatchRequest,
    options: KitaruRequestOptions = {},
  ): Promise<SessionNodeResponse[]> {
    return this.sessions.upsertNodes(sessionId, request, options);
  }

  async getReplay(
    replayId: string,
    options: KitaruRequestOptions = {},
  ): Promise<ReplayResponse> {
    return this.replays.get(replayId, options);
  }

  async getTaskSpec(
    taskId: string,
    options: KitaruRequestOptions = {},
  ): Promise<TaskSpecResponse> {
    return this.tasks.getSpec(taskId, options);
  }

  async lookupToolResult(
    replayId: string,
    request: ToolLookupRequest,
    options: KitaruRequestOptions = {},
  ): Promise<ToolLookupResponse> {
    return this.replays.toolLookup(replayId, request, options);
  }
}
