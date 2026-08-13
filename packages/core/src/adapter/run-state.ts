import type { KitaruClient } from "../client.js";
import type { JsonValue, ReplaySpec } from "../types.js";

export type AdapterClient = Pick<
  KitaruClient,
  | "createSession"
  | "getReplay"
  | "getTaskSpec"
  | "lookupToolResult"
  | "updateSession"
  | "upsertSessionNodes"
>;

interface ToolLedgerEntry {
  callId: string;
  error?: { message: string; name: string };
  inputs: JsonValue;
  inputsLossy?: boolean;
  mocked: boolean;
  outcome: "completed" | "failed" | "pending";
  output?: JsonValue;
  policy?: "history" | "static";
  startedAt?: string;
  toolName: string;
}

interface RunStateOptions {
  client: AdapterClient;
  effectiveInput: JsonValue;
  effectiveModelSettings?: Record<string, JsonValue>;
  replayId?: string;
  requestedModelId: string;
  rootIndex?: number;
  sessionId: string;
  spec?: ReplaySpec;
}

export interface AdapterRunState {
  readonly client: AdapterClient;
  readonly effectiveInput: JsonValue;
  readonly effectiveModelSettings?: Record<string, JsonValue>;
  readonly failure: unknown;
  readonly replayId?: string;
  readonly requestedModelId: string;
  readonly rootIndex: number;
  readonly sessionId: string;
  readonly spec?: ReplaySpec;
  allocateNode(): { index: number };
  awaitSteps(): Promise<void>;
  clearLedger(callIds: readonly string[]): void;
  enqueueStep(operation: () => Promise<void>): Promise<void>;
  failedLedgerEntries(): ToolLedgerEntry[];
  getToolCall(callId: string): ToolLedgerEntry | undefined;
  setToolCall(entry: ToolLedgerEntry): void;
  storeFailure(error: unknown): void;
  takeStepStart(endedAt: string): string;
}

export class RunState implements AdapterRunState {
  readonly client: AdapterClient;
  readonly effectiveInput: JsonValue;
  readonly effectiveModelSettings?: Record<string, JsonValue>;
  readonly replayId?: string;
  readonly requestedModelId: string;
  readonly rootIndex: number;
  readonly sessionId: string;
  readonly spec?: ReplaySpec;

  #failure: unknown;
  #ledger = new Map<string, ToolLedgerEntry>();
  #nextIndex: number;
  #stepBoundary: string = new Date().toISOString();
  #stepFailure: unknown;
  #stepTail: Promise<void> = Promise.resolve();

  constructor(options: RunStateOptions) {
    this.client = options.client;
    this.effectiveInput = options.effectiveInput;
    this.effectiveModelSettings = options.effectiveModelSettings;
    this.replayId = options.replayId;
    this.requestedModelId = options.requestedModelId;
    this.rootIndex = options.rootIndex ?? 0;
    this.#nextIndex = this.rootIndex + 1;
    this.sessionId = options.sessionId;
    this.spec = options.spec;
  }

  allocateNode(): { index: number } {
    const allocation = { index: this.#nextIndex };
    this.#nextIndex += 1;
    return allocation;
  }

  enqueueStep(operation: () => Promise<void>): Promise<void> {
    const pending = this.#stepTail.then(operation);
    // Each batch upsert is independent and idempotent, so one failed step must
    // not stop the queue from attempting the steps that follow it.
    this.#stepTail = pending.catch((error: unknown) => {
      if (this.#stepFailure === undefined) {
        this.#stepFailure = error;
      }
    });
    return pending;
  }

  async awaitSteps(): Promise<void> {
    await this.#stepTail;
    if (this.#stepFailure !== undefined) {
      throw this.#stepFailure;
    }
  }

  // Steps run one after another, so the previous step's end is the earliest
  // known start for the next one, and run creation anchors the first step.
  takeStepStart(endedAt: string): string {
    const startedAt = this.#stepBoundary;
    this.#stepBoundary = endedAt;
    return Date.parse(startedAt) <= Date.parse(endedAt) ? startedAt : endedAt;
  }

  clearLedger(callIds: readonly string[]): void {
    for (const callId of callIds) {
      this.#ledger.delete(callId);
    }
  }

  failedLedgerEntries(): ToolLedgerEntry[] {
    return [...this.#ledger.values()].filter(
      (entry) => entry.outcome === "failed",
    );
  }

  get failure(): unknown {
    return this.#failure;
  }

  getToolCall(callId: string): ToolLedgerEntry | undefined {
    return this.#ledger.get(callId);
  }

  setToolCall(entry: ToolLedgerEntry): void {
    this.#ledger.set(entry.callId, entry);
  }

  storeFailure(error: unknown): void {
    if (this.#failure === undefined) {
      this.#failure = error;
    }
  }
}
