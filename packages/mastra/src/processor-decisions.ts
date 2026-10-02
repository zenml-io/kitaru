import { AsyncLocalStorage } from "node:async_hooks";
import type { JsonValue, SessionNodeCreateRequest } from "@zenml-io/kitaru";
import {
  boundedRecorderConversion,
  providerFamily,
  ROOT_NODE_EXTERNAL_ID,
  resolveCost,
  type SecretKeyClassifier,
} from "@zenml-io/kitaru/adapter";
import { decodeMemoryValue, encodeMemoryValue } from "./memory-snapshot.js";
import { getModelTokens } from "./model-usage.js";
import { describeProviderError } from "./provider-errors.js";
import type { KitaruCostCalculator } from "./types.js";

/** A named, once-per-turn application decision. */
export interface ProcessorDecision {
  run<T>(callback: () => T | Promise<T>): Promise<T>;
  /** Instrument a public AI SDK model used inside this decision's callback. */
  instrumentModel<T extends object>(model: T): T;
}

export interface ProcessorDecisions {
  /** Declare every decision while constructing the turn's agent. */
  define(name: string): ProcessorDecision;
}

export interface ProcessorDecisionSnapshot {
  [key: string]: JsonValue;
  version: 1;
  complete: boolean;
  declared: string[];
  entries: Array<{ name: string; output: JsonValue }>;
}

export interface ProcessorDecisionOptions {
  mode: "live" | "pinned";
  recorded?: unknown;
  invocationId: string;
  recordNode?: (node: SessionNodeCreateRequest) => Promise<void>;
  captureError: (error: unknown) => void;
  sanitizeEvidence?: <T>(value: T) => T;
  isSecretKey?: SecretKeyClassifier;
  costCalculator?: KitaruCostCalculator;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

/** Capture processor decisions without waiting for diagnostic writes. */
export function createProcessorDecisions(options: ProcessorDecisionOptions) {
  const definitions = new Map<string, ProcessorDecision>();
  const calls = new Map<string, number>();
  const entries = new Map<string, JsonValue>();
  const pinned = new Map<string, unknown>();
  const context = new AsyncLocalStorage<{ name: string; externalId: string }>();
  const pending = new Set<Promise<void>>();
  let complete = true;
  let validated = false;
  let ordinal = 0;
  let finished = false;

  function captureError(error: unknown): void {
    try {
      options.captureError(error);
    } catch {
      // Diagnostic callbacks cannot change application results.
    }
  }

  function incomplete(reason: string): void {
    complete = false;
    captureError(new Error(`Processor decision capture incomplete: ${reason}`));
  }

  function queue(work: () => Promise<void>): void {
    if (!options.recordNode || finished) return;
    const task = Promise.resolve()
      .then(work)
      .catch((error) => {
        if (!finished) complete = false;
        captureError(error);
      });
    pending.add(task);
    void task.then(() => pending.delete(task));
  }

  function evidence(value: unknown, path: string): JsonValue {
    const sanitized = options.sanitizeEvidence
      ? options.sanitizeEvidence(value)
      : value;
    const recorded = boundedRecorderConversion(
      sanitized,
      path,
      undefined,
      options.isSecretKey,
    );
    if (recorded.lossy) incomplete(`${path} was truncated or degraded`);
    return recorded.value;
  }

  function safeEvidence(value: unknown, path: string): JsonValue {
    try {
      return evidence(value, path);
    } catch (error) {
      incomplete(path);
      captureError(error);
      return null;
    }
  }

  function getError(error: unknown): string {
    try {
      return describeProviderError(error) ?? "Processor decision failed";
    } catch {
      return "Processor decision failed";
    }
  }

  function validatePinned(): void {
    if (validated) return;
    if (options.mode === "pinned") {
      const recorded = asRecord(options.recorded);
      if (
        recorded.version !== 1 ||
        recorded.complete !== true ||
        !Array.isArray(recorded.declared) ||
        !Array.isArray(recorded.entries)
      )
        throw new Error(
          "Pinned processor decisions require a complete recording",
        );
      const declared = recorded.declared;
      if (
        declared.some((name) => typeof name !== "string") ||
        new Set(declared).size !== declared.length ||
        declared.length !== definitions.size ||
        declared.some((name) => !definitions.has(name)) ||
        recorded.entries.length !== declared.length
      )
        throw new Error("Pinned processor decision declarations do not match");
      for (const raw of recorded.entries) {
        const entry = asRecord(raw);
        if (
          typeof entry.name !== "string" ||
          !definitions.has(entry.name) ||
          pinned.has(entry.name) ||
          !Object.hasOwn(entry, "output")
        )
          throw new Error("Malformed pinned processor decision result");
        pinned.set(
          entry.name,
          decodeMemoryValue(entry.output as JsonValue, options.isSecretKey),
        );
      }
    }
    validated = true;
  }

  const binding: ProcessorDecisions = {
    define(name) {
      if (validated)
        throw new Error("Declare processor decisions in the agent factory");
      if (!name.trim() || name.length > 200)
        throw new Error(
          "Processor decision names must contain 1 to 200 characters",
        );
      const existing = definitions.get(name);
      if (existing) return existing;
      const decision: ProcessorDecision = {
        async run(callback) {
          if (!options.recordNode && options.mode === "live") return callback();
          if (!validated)
            throw new Error(
              "Processor decisions must be validated before execution",
            );
          const callCount = (calls.get(name) ?? 0) + 1;
          calls.set(name, callCount);
          if (callCount > 1) {
            if (options.mode === "pinned")
              throw new Error(
                `Pinned processor decision '${name}' ran more than once`,
              );
            incomplete(`decision '${name}' ran more than once`);
          }
          const externalId = `${options.invocationId}:decision:${ordinal++}`;
          const startedAt = new Date().toISOString();
          let failed = false;
          let error: unknown;
          let result: unknown;
          try {
            if (options.mode === "pinned") {
              result = pinned.get(name);
            } else {
              result = await context.run({ name, externalId }, callback);
            }
            try {
              if (!finished)
                entries.set(
                  name,
                  encodeMemoryValue(result, undefined, options.isSecretKey),
                );
            } catch (captureFailure) {
              incomplete(`decision '${name}' result could not be encoded`);
              captureError(captureFailure);
            }
            return result as Awaited<ReturnType<typeof callback>>;
          } catch (failure) {
            failed = true;
            error = failure;
            complete = false;
            throw failure;
          } finally {
            const endedAt = new Date().toISOString();
            const outputs = failed
              ? null
              : safeEvidence(result, `processor decision '${name}' result`);
            const recordedError = failed ? getError(error) : null;
            queue(async () => {
              if (finished) return;
              await options.recordNode?.({
                name,
                node_type: "span",
                external_id: externalId,
                parent_external_id: ROOT_NODE_EXTERNAL_ID,
                started_at: startedAt,
                ended_at: endedAt,
                status: failed ? "failed" : "completed",
                error: recordedError,
                inputs: null,
                outputs,
                attributes: { processor_decision: true, mode: options.mode },
              });
            });
          }
        },
        instrumentModel(model) {
          if (!options.recordNode && options.mode === "live") return model;
          const native = asRecord(model);
          try {
            if (
              typeof native.doGenerate !== "function" &&
              typeof native.doStream !== "function"
            ) {
              if (options.mode === "pinned")
                throw new TypeError(
                  "Pinned processor decisions require a public AI SDK model",
                );
              incomplete("model has no public doGenerate or doStream method");
              return model;
            }
            if (
              !["v2", "v3", "v4"].includes(String(native.specificationVersion))
            ) {
              if (options.mode === "pinned")
                throw new TypeError(
                  "Pinned processor decisions require a v2, v3, or v4 model",
                );
              incomplete("model specification must be v2, v3, or v4");
              return model;
            }
          } catch (error) {
            if (options.mode === "pinned") throw error;
            incomplete("model capability evidence could not be read");
            captureError(error);
            return model;
          }
          return new Proxy(model, {
            get(target, key) {
              const value = Reflect.get(target, key, target);
              if (
                (key === "doGenerate" || key === "doStream") &&
                typeof value === "function"
              ) {
                return async (...args: unknown[]) => {
                  const input = args[0];
                  const active = context.getStore();
                  if (options.mode === "pinned")
                    throw new Error(
                      "Pinned processor decisions cannot call a live classifier",
                    );
                  if (!active || active.name !== name) {
                    incomplete(`model called outside decision '${name}'`);
                    return Reflect.apply(value, target, args);
                  }
                  if (key === "doStream") {
                    incomplete(
                      `streaming classifier '${name}' is unsupported for capture`,
                    );
                    return Reflect.apply(value, target, args);
                  }
                  const startedAt = new Date().toISOString();
                  const externalId = `${options.invocationId}:classifier:${ordinal++}`;
                  let inputs: JsonValue = null;
                  let parameters: Record<string, JsonValue> = {};
                  try {
                    inputs = safeEvidence(
                      asRecord(input).prompt,
                      "processor classifier prompt",
                    );
                    parameters = Object.fromEntries(
                      [
                        "maxOutputTokens",
                        "temperature",
                        "topP",
                        "topK",
                        "presencePenalty",
                        "frequencyPenalty",
                        "stopSequences",
                        "seed",
                        "responseFormat",
                      ].flatMap((key) =>
                        asRecord(input)[key] === undefined
                          ? []
                          : [
                              [
                                key,
                                safeEvidence(
                                  asRecord(input)[key],
                                  `processor classifier ${key}`,
                                ),
                              ],
                            ],
                      ),
                    );
                  } catch (error) {
                    incomplete("classifier input evidence could not be read");
                    captureError(error);
                  }
                  let result: unknown;
                  let failure: unknown;
                  let failed = false;
                  try {
                    result = await Reflect.apply(value, target, args);
                    return result;
                  } catch (error) {
                    failed = true;
                    failure = error;
                    throw error;
                  } finally {
                    try {
                      const endedAt = new Date().toISOString();
                      const output = asRecord(result);
                      const requested =
                        typeof native.modelId === "string"
                          ? native.modelId
                          : "";
                      const provider =
                        typeof native.provider === "string"
                          ? native.provider
                          : "";
                      const served = asRecord(output.response).modelId;
                      const modelId =
                        typeof served === "string" ? served : requested;
                      const tokens = getModelTokens(output.usage);
                      const finishReason =
                        typeof output.finishReason === "string"
                          ? output.finishReason
                          : asRecord(output.finishReason).unified;
                      const outputs = failed
                        ? null
                        : safeEvidence(
                            {
                              content: output.content ?? null,
                              finish_reason:
                                typeof finishReason === "string"
                                  ? finishReason
                                  : null,
                            },
                            "processor classifier result",
                          );
                      const recordedError = failed ? getError(failure) : null;
                      queue(async () => {
                        const cost = await resolveCost(options.costCalculator, {
                          model: modelId,
                          provider,
                          requestedModelId: requested,
                          tokens,
                        });
                        if (finished) return;
                        await options.recordNode?.({
                          name: `${name}:classifier`,
                          node_type: "llm_call",
                          external_id: externalId,
                          parent_external_id: active.externalId,
                          started_at: startedAt,
                          ended_at: endedAt,
                          status: failed ? "failed" : "completed",
                          error: recordedError,
                          model: modelId,
                          requested_model: requested,
                          model_provider: providerFamily(provider),
                          tokens,
                          model_params: parameters,
                          cost: cost.cost,
                          inputs,
                          outputs,
                          attributes: {
                            processor_decision: name,
                            provider_id: provider,
                            cost: cost.attribute,
                          },
                        });
                      });
                    } catch (error) {
                      incomplete("classifier evidence could not be captured");
                      captureError(error);
                    }
                  }
                };
              }
              return typeof value === "function" ? value.bind(target) : value;
            },
          });
        },
      };
      definitions.set(name, decision);
      return decision;
    },
  };

  return {
    binding,
    validatePinned,
    async finish(waitMs = 1000): Promise<void> {
      if (finished) return;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          (async () => {
            while (pending.size > 0) await Promise.all(pending);
          })(),
          new Promise<void>((resolve) => {
            timer = setTimeout(() => {
              incomplete("diagnostic writes did not finish in time");
              resolve();
            }, waitMs);
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
        finished = true;
      }
    },
    snapshot(): ProcessorDecisionSnapshot {
      return {
        version: 1,
        complete:
          complete &&
          [...definitions.keys()].every(
            (name) => calls.get(name) === 1 && entries.has(name),
          ),
        declared: [...definitions.keys()],
        entries: [...entries].map(([name, output]) => ({ name, output })),
      };
    },
  };
}
