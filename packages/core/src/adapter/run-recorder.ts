import { writeFile } from "node:fs/promises";
import { KitaruApiError } from "../errors.js";
import type {
  JsonValue,
  ReplaySpec,
  SessionNodeCreateRequest,
} from "../types.js";
import { recordedPayloadJson } from "./recorded-json.js";
import {
  type AdapterClient,
  type AdapterRunState,
  ROOT_NODE_EXTERNAL_ID,
  RunState,
} from "./run-state.js";
import { flushFailedPolicyOutcomes } from "./step.js";

function errorText(error: unknown): string {
  if (error instanceof Error) {
    return error.message || error.name;
  }
  return String(error);
}

function rootNode(
  state: AdapterRunState,
  options: {
    endedAt?: string;
    error?: string;
    inputs?: JsonValue;
    output?: JsonValue;
    startedAt: string;
    status: "completed" | "failed" | "in_progress";
  },
): SessionNodeCreateRequest {
  return {
    attributes: {},
    ended_at: options.endedAt,
    error: options.error,
    external_id: ROOT_NODE_EXTERNAL_ID,
    inputs:
      options.inputs === undefined ? state.effectiveInput : options.inputs,
    name: "run",
    node_type: "span",
    outputs: options.output ?? null,
    parent_external_id: null,
    started_at: options.startedAt,
    status: options.status,
  };
}

async function bestEffort(operation: () => Promise<unknown>): Promise<void> {
  try {
    await operation();
  } catch {
    // Cleanup must not replace the original runtime or recording failure.
  }
}

export interface RunRecorderOptions {
  adapterVersion: string;
  agentId: string;
  agentVersionId?: string;
  client: AdapterClient;
  effectiveInput: JsonValue;
  effectiveModelSettings?: Record<string, JsonValue>;
  framework: string;
  metadata?: Record<string, JsonValue>;
  name?: string;
  replayId?: string;
  requestedModelId: string;
  sessionIdFile?: string;
  spec?: ReplaySpec;
  startedAt?: string;
}

interface CompletionOptions {
  inputs?: JsonValue;
  metadata?: Record<string, JsonValue>;
  /**
   * Metadata to store instead when the server refuses `inputs` or
   * `metadata`, so the completed session can say why it lacks them.
   */
  rejectedMetadata?: Record<string, JsonValue>;
}

export interface RunCompletion {
  /** Whether the server stored the inputs and metadata passed to `complete`. */
  finalizationAccepted: boolean;
}

export class RunRecorder {
  readonly state: AdapterRunState;

  readonly #client: AdapterClient;
  readonly #sessionIdFile?: string;
  readonly #startedAt: string;

  private constructor(options: {
    client: AdapterClient;
    sessionIdFile?: string;
    startedAt: string;
    state: AdapterRunState;
  }) {
    this.#client = options.client;
    this.#sessionIdFile = options.sessionIdFile;
    this.#startedAt = options.startedAt;
    this.state = options.state;
  }

  static async create(options: RunRecorderOptions): Promise<RunRecorder> {
    const startedAt = options.startedAt ?? new Date().toISOString();
    const session = await options.client.createSession({
      adapter_version: options.adapterVersion,
      agent_id: options.agentId,
      agent_version_id: options.agentVersionId,
      framework: options.framework,
      inputs: options.effectiveInput,
      ...(options.metadata ? { metadata: options.metadata } : {}),
      name: options.name,
      origin: options.replayId ? "replay" : "recorded",
      outputs: null,
      started_at: startedAt,
      status: "in_progress",
    });
    const state = new RunState({
      client: options.client,
      effectiveInput: options.effectiveInput,
      effectiveModelSettings: options.effectiveModelSettings,
      replayId: options.replayId,
      requestedModelId: options.requestedModelId,
      sessionId: session.id,
      spec: options.spec,
    });
    return new RunRecorder({
      client: options.client,
      sessionIdFile: options.sessionIdFile,
      startedAt,
      state,
    });
  }

  async initialize(): Promise<void> {
    await this.#client.upsertSessionNodes(this.state.sessionId, {
      nodes: [
        rootNode(this.state, {
          startedAt: this.#startedAt,
          status: "in_progress",
        }),
      ],
    });
    if (this.#sessionIdFile) {
      await writeFile(this.#sessionIdFile, this.state.sessionId, "utf8");
    }
  }

  async complete(
    result: unknown,
    options: CompletionOptions = {},
  ): Promise<RunCompletion> {
    await this.state.awaitSteps();
    return this.#closeCompleted(result, options);
  }

  /**
   * Record a run that finished but whose recording is incomplete.
   *
   * The session is completed with the run's result even when step uploads
   * failed, and `metadata` states why the recording is incomplete.
   */
  async completeIncompleteRecording(
    result: unknown,
    metadata: Record<string, JsonValue>,
  ): Promise<RunCompletion> {
    await bestEffort(() => this.state.awaitSteps());
    return this.#closeCompleted(result, { metadata });
  }

  async #closeCompleted(
    result: unknown,
    options: CompletionOptions,
  ): Promise<RunCompletion> {
    // The run has finished by the time its result is recorded, so a result too
    // large or too circular to record is bounded instead of turning a
    // successful generation into a failed one.
    const serializedOutput = recordedPayloadJson(result, "run output");
    const endedAt = new Date().toISOString();
    await this.#client.upsertSessionNodes(this.state.sessionId, {
      nodes: [
        rootNode(this.state, {
          endedAt,
          inputs: options.inputs,
          output: serializedOutput,
          startedAt: this.#startedAt,
          status: "completed",
        }),
      ],
    });
    const completion = {
      ended_at: endedAt,
      outputs: serializedOutput,
      status: "completed" as const,
    };
    const finalization = {
      ...("inputs" in options ? { inputs: options.inputs } : {}),
      ...(options.metadata ? { metadata: options.metadata } : {}),
    };
    try {
      await this.#client.updateSession(this.state.sessionId, {
        ...completion,
        ...finalization,
      });
      return { finalizationAccepted: true };
    } catch (error) {
      // A server that predates these fields, or that refuses the replay
      // inputs, answers 422 before it writes anything. The run itself
      // succeeded, so it is still recorded as completed with its outputs,
      // just without replay inputs.
      if (
        Object.keys(finalization).length === 0 ||
        !(error instanceof KitaruApiError) ||
        error.status !== 422
      )
        throw error;
    }
    await this.#client.updateSession(this.state.sessionId, {
      ...completion,
      ...(options.rejectedMetadata
        ? { metadata: options.rejectedMetadata }
        : {}),
    });
    return { finalizationAccepted: false };
  }

  async fail(
    error: unknown,
    metadata?: Record<string, JsonValue>,
  ): Promise<void> {
    this.state.storeFailure(error);
    // Let queued step writes land before the failed ledger and the closing
    // node, so a late step cannot arrive after the session is marked failed.
    await bestEffort(() => this.state.awaitSteps());
    await bestEffort(() => flushFailedPolicyOutcomes(this.state));
    await this.#closeFailed(error, metadata);
  }

  async failRecording(
    error: unknown,
    metadata?: Record<string, JsonValue>,
  ): Promise<void> {
    // A telemetry failure must not enter application state. Tool hooks use
    // state.failure to stop execution after policy or runtime failures.
    await bestEffort(() => this.state.awaitSteps());
    await this.#closeFailed(error, metadata);
  }

  async #closeFailed(
    error: unknown,
    metadata?: Record<string, JsonValue>,
  ): Promise<void> {
    const endedAt = new Date().toISOString();
    await bestEffort(() =>
      this.#client.upsertSessionNodes(this.state.sessionId, {
        nodes: [
          rootNode(this.state, {
            endedAt,
            error: errorText(error),
            startedAt: this.#startedAt,
            status: "failed",
          }),
        ],
      }),
    );
    await bestEffort(() =>
      this.#client.updateSession(this.state.sessionId, {
        ended_at: endedAt,
        error: errorText(error),
        ...(metadata ? { metadata } : {}),
        status: "failed",
      }),
    );
  }
}
