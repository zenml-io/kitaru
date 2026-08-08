import { writeFile } from "node:fs/promises";

import { toRecorderJson } from "../json.js";
import type {
  JsonValue,
  ReplaySpec,
  SessionNodeCreateRequest,
} from "../types.js";
import {
  type AdapterClient,
  type AdapterRunState,
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
    output?: JsonValue;
    startedAt: string;
    status: "completed" | "failed" | "in_progress";
  },
): SessionNodeCreateRequest {
  return {
    attributes: {},
    ended_at: options.endedAt,
    error: options.error,
    index: state.rootIndex,
    inputs: state.effectiveInput,
    name: "run",
    node_type: "span",
    outputs: options.output ?? null,
    parent_index: null,
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
  name?: string;
  replayId?: string;
  requestedModelId: string;
  sessionIdFile?: string;
  spec?: ReplaySpec;
  startedAt?: string;
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

  async complete(result: unknown): Promise<void> {
    await this.state.awaitSteps();
    const serializedOutput = toRecorderJson(result);
    const endedAt = new Date().toISOString();
    await this.#client.upsertSessionNodes(this.state.sessionId, {
      nodes: [
        rootNode(this.state, {
          endedAt,
          output: serializedOutput,
          startedAt: this.#startedAt,
          status: "completed",
        }),
      ],
    });
    await this.#client.updateSession(this.state.sessionId, {
      ended_at: endedAt,
      outputs: serializedOutput,
      status: "completed",
    });
  }

  async fail(error: unknown): Promise<void> {
    this.state.storeFailure(error);
    const endedAt = new Date().toISOString();
    await bestEffort(() => flushFailedPolicyOutcomes(this.state));
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
        status: "failed",
      }),
    );
  }
}
