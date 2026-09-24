import { createHash } from "node:crypto";
import type { MastraDBMessage, StorageThreadType } from "@mastra/core/memory";
import {
  MASTRA_AUTH_TOKEN_KEY,
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
} from "@mastra/core/request-context";
import type {
  ObservationalMemoryRecord,
  StorageResourceType,
} from "@mastra/core/storage";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  MAX_MASTRA_REPLAY_ITEMS,
  MAX_MASTRA_REPLAY_JSON_BYTES,
  strictMastraReplayValue,
} from "@zenml-io/kitaru/adapter";
import { fileReference } from "./stateful-files.js";

export const MEMORY_REPLAY_KEY = "mastra_memory_replay";
const CODEC_KEY = "$mastra";

export interface MastraMemorySnapshot {
  threadId: string;
  resourceId: string;
  thread: StorageThreadType | null;
  resource: StorageResourceType | null;
  messages: MastraDBMessage[];
  records: ObservationalMemoryRecord[];
}

export interface MastraRecordedFile {
  url: string;
  mediaType: string;
  bytes: Uint8Array;
}

export interface MastraFileManifestEntry {
  [key: string]: JsonValue;
  url: string;
  mediaType: string;
  base64: string;
  length: number;
  sha256: string;
}

export interface MastraMemoryReplayInput {
  invocationId: string;
  rawInput: unknown;
  initialSnapshot: MastraMemorySnapshot;
  /** Materialized options only. Models and schemas need explicit JSON representations. */
  configuration: Record<string, unknown>;
  requestContext: Record<string, unknown>;
  files: MastraRecordedFile[];
  omTape?: JsonValue[];
}

export interface MastraMemoryReplayEnvelope {
  [key: string]: JsonValue;
  version: 3;
  complete: boolean;
  reasons: string[];
  invocationId: string;
  rawInput: JsonValue;
  initialSnapshot: JsonValue;
  configuration: JsonValue;
  requestContext: JsonValue;
  files: MastraFileManifestEntry[];
  omTape: JsonValue[];
}

class MemoryReplayError extends Error {}

function unsupported(reason: string): Error {
  return new MemoryReplayError(`Unsupported Mastra memory replay: ${reason}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireValue(condition: unknown, reason: string): asserts condition {
  if (!condition) throw unsupported(reason);
}

function hash(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function binary(bytes: Uint8Array): {
  base64: string;
  length: number;
  sha256: string;
} {
  requireValue(
    bytes.byteLength <= 8 * 1_048_576,
    "Binary content exceeds maximum file bytes 8388608.",
  );
  return {
    base64: Buffer.from(bytes).toString("base64"),
    length: bytes.byteLength,
    sha256: hash(bytes),
  };
}

function readBinary(value: Record<string, unknown>): Uint8Array {
  requireValue(
    typeof value.base64 === "string" &&
      typeof value.length === "number" &&
      Number.isSafeInteger(value.length) &&
      value.length >= 0 &&
      value.length <= 8 * 1_048_576 &&
      typeof value.sha256 === "string",
    "Malformed binary content.",
  );
  const bytes = new Uint8Array(Buffer.from(value.base64, "base64"));
  requireValue(
    Buffer.from(bytes).toString("base64") === value.base64 &&
      bytes.length === value.length &&
      hash(bytes) === value.sha256,
    "Corrupt binary content hash, length, or encoding.",
  );
  return bytes;
}

function validateUrl(value: string): URL {
  const url = new URL(value);
  if (url.protocol === "kitaru-file:") {
    requireValue(
      /^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/.test(value),
      "Malformed captured file reference.",
    );
    return url;
  }
  requireValue(
    (url.protocol === "https:" || url.protocol === "http:") &&
      !url.username &&
      !url.password,
    "URL credentials are not replayable.",
  );
  requireValue(
    ![...url.searchParams.keys()].some((key) =>
      /(?:^|[-_])(?:api[-_]?key|authorization|cookie|password|secret|token|signature|credential|sig)$/i.test(
        key,
      ),
    ),
    "URL query credentials are not replayable.",
  );
  return url;
}

/** Encode the few non-JSON values in native memory without losing their types. */
export function encodeMemoryValue(value: unknown): JsonValue {
  let items = 0;
  const active = new Set<object>();
  function visit(current: unknown, depth: number): JsonValue {
    requireValue(
      ++items <= MAX_MASTRA_REPLAY_ITEMS && depth < 64,
      `Memory value exceeds maximum items ${MAX_MASTRA_REPLAY_ITEMS} or depth 64.`,
    );
    if (current === undefined) return { [CODEC_KEY]: "undefined" };
    if (current === null || typeof current === "boolean") return current;
    if (typeof current === "string") {
      requireValue(
        current.length <= MAX_MASTRA_REPLAY_JSON_BYTES,
        `Memory value exceeds maximum string length ${MAX_MASTRA_REPLAY_JSON_BYTES}.`,
      );
      return current;
    }
    if (typeof current === "number") {
      requireValue(Number.isFinite(current), "Non-finite memory number.");
      return current;
    }
    requireValue(
      typeof current === "object",
      "Unsupported memory value; functions and live dependencies need explicit representations.",
    );
    if (current instanceof Date) {
      requireValue(Number.isFinite(current.getTime()), "Invalid memory Date.");
      return { [CODEC_KEY]: "date", value: current.toISOString() };
    }
    if (current instanceof URL) {
      validateUrl(current.href);
      return { [CODEC_KEY]: "url", value: current.href };
    }
    if (current instanceof Uint8Array)
      return { [CODEC_KEY]: "bytes", ...binary(current) };
    requireValue(!active.has(current), "Circular memory value.");
    active.add(current);
    try {
      if (Array.isArray(current))
        return current.map((item) => visit(item, depth + 1));
      requireValue(
        Object.getPrototypeOf(current) === Object.prototype ||
          Object.getPrototypeOf(current) === null,
        "Unsupported memory object; use explicit JSON configuration.",
      );
      requireValue(
        !Object.hasOwn(current, CODEC_KEY) &&
          Reflect.ownKeys(current).every((key) => typeof key === "string"),
        "Reserved or symbolic memory key.",
      );
      const result: Record<string, JsonValue> = Object.create(null);
      for (const [key, descriptor] of Object.entries(
        Object.getOwnPropertyDescriptors(current),
      )) {
        requireValue(
          descriptor.enumerable && "value" in descriptor,
          "Accessor or hidden memory properties are unsupported.",
        );
        result[key] = visit(descriptor.value, depth + 1);
      }
      return result;
    } finally {
      active.delete(current);
    }
  }
  const encoded = visit(value, 0);
  return strictMastraReplayValue(encoded);
}

/** Decode an already bounded value, rejecting ambiguous or damaged codec records. */
export function decodeMemoryValue(value: JsonValue): unknown {
  const converted = strictMastraReplayValue(value);
  function visit(current: JsonValue): unknown {
    if (Array.isArray(current)) return current.map(visit);
    if (!isRecord(current)) return current;
    if (Object.hasOwn(current, CODEC_KEY)) {
      const kind = current[CODEC_KEY];
      if (kind === "undefined" && Object.keys(current).length === 1)
        return undefined;
      if (
        (kind === "date" || kind === "url") &&
        Object.keys(current).length === 2 &&
        typeof current.value === "string"
      ) {
        if (kind === "url") return validateUrl(current.value);
        const date = new Date(current.value);
        requireValue(
          Number.isFinite(date.getTime()) &&
            date.toISOString() === current.value,
          "Malformed memory Date.",
        );
        return date;
      }
      if (kind === "bytes" && Object.keys(current).length === 4)
        return readBinary(current);
      throw unsupported("Malformed memory codec tag.");
    }
    return Object.fromEntries(
      Object.entries(current).map(([key, item]) => [
        key,
        visit(item as JsonValue),
      ]),
    );
  }
  return visit(converted);
}

function readStoredDates(value: unknown, keys: readonly string[]): unknown {
  if (!isRecord(value)) return value;
  const copy = { ...value };
  for (const key of keys) {
    const stored = copy[key];
    if (typeof stored !== "string") continue;
    const date = new Date(stored);
    // Only an exact ISO rendering is a serialized Date; other strings stay
    // unchanged so validation still rejects them.
    if (Number.isFinite(date.getTime()) && date.toISOString() === stored)
      copy[key] = date;
  }
  return copy;
}

/**
 * Convert ISO-string dates at known snapshot fields back to Dates.
 *
 * SQL stores keep buffered observation chunks in a JSON column, so reading a
 * record back returns each chunk's `createdAt` and `lastObservedAt` as strings.
 */
export function normalizeStoredMemoryDates(snapshot: unknown): unknown {
  if (!isRecord(snapshot)) return snapshot;
  const timestamps = ["createdAt", "updatedAt"];
  const records = Array.isArray(snapshot.records)
    ? snapshot.records.map((value) => {
        const record = readStoredDates(value, [
          ...timestamps,
          "lastObservedAt",
          "lastBufferedAtTime",
        ]);
        if (
          !isRecord(record) ||
          !Array.isArray(record.bufferedObservationChunks)
        )
          return record;
        return {
          ...record,
          bufferedObservationChunks: record.bufferedObservationChunks.map(
            (chunk) => readStoredDates(chunk, ["createdAt", "lastObservedAt"]),
          ),
        };
      })
    : snapshot.records;
  return {
    ...snapshot,
    thread: readStoredDates(snapshot.thread, timestamps),
    resource: readStoredDates(snapshot.resource, timestamps),
    messages: Array.isArray(snapshot.messages)
      ? snapshot.messages.map((message) =>
          readStoredDates(message, ["createdAt"]),
        )
      : snapshot.messages,
    records,
  };
}

/** Validate the complete native state before an isolated store receives any writes. */
export function validateMemorySnapshot(
  value: unknown,
): asserts value is MastraMemorySnapshot {
  requireValue(
    isRecord(value) &&
      typeof value.threadId === "string" &&
      value.threadId.length > 0 &&
      typeof value.resourceId === "string" &&
      value.resourceId.length > 0 &&
      Array.isArray(value.messages) &&
      Array.isArray(value.records),
    "Malformed initial memory snapshot.",
  );
  const dates = (record: Record<string, unknown>) =>
    record.createdAt instanceof Date && record.updatedAt instanceof Date;
  requireValue(
    value.thread === null ||
      (isRecord(value.thread) &&
        value.thread.id === value.threadId &&
        value.thread.resourceId === value.resourceId &&
        dates(value.thread)),
    "Malformed or mismatched thread record.",
  );
  requireValue(
    value.resource === null ||
      (isRecord(value.resource) &&
        value.resource.id === value.resourceId &&
        dates(value.resource)),
    "Malformed or mismatched resource record.",
  );
  requireValue(
    value.thread !== null ||
      (value.messages.length === 0 && value.records.length === 0),
    "Orphaned memory state.",
  );
  const messageIds = new Set<string>();
  for (const message of value.messages) {
    requireValue(
      isRecord(message) &&
        typeof message.id === "string" &&
        !messageIds.has(message.id) &&
        message.threadId === value.threadId &&
        (message.resourceId === undefined ||
          message.resourceId === value.resourceId) &&
        message.createdAt instanceof Date &&
        ["system", "user", "assistant", "tool"].includes(
          String(message.role),
        ) &&
        isRecord(message.content) &&
        message.content.format === 2 &&
        Array.isArray(message.content.parts),
      "Malformed or mismatched stored message.",
    );
    messageIds.add(message.id);
  }
  const recordIds = new Set<string>();
  for (const record of value.records) {
    requireValue(
      isRecord(record) &&
        typeof record.id === "string" &&
        !recordIds.has(record.id) &&
        record.scope === "thread" &&
        record.threadId === value.threadId &&
        record.resourceId === value.resourceId &&
        dates(record) &&
        typeof record.activeObservations === "string" &&
        isRecord(record.config),
      "Malformed or unsupported observational-memory record.",
    );
    recordIds.add(record.id);
    for (const key of [
      "generationCount",
      "totalTokensObserved",
      "observationTokenCount",
      "pendingMessageTokens",
      "lastBufferedAtTokens",
    ])
      requireValue(
        typeof record[key] === "number" &&
          Number.isFinite(record[key]) &&
          record[key] >= 0,
        "Malformed observational-memory counter.",
      );
    for (const key of [
      "isObserving",
      "isReflecting",
      "isBufferingObservation",
      "isBufferingReflection",
    ])
      requireValue(
        record[key] === false,
        "Unjoined observational-memory work or missing work flag.",
      );
    requireValue(
      record.lastBufferedAtTime === null ||
        record.lastBufferedAtTime instanceof Date,
      "Malformed observational-memory buffer cursor.",
    );
    requireValue(
      record.lastObservedAt === undefined ||
        record.lastObservedAt instanceof Date,
      "Malformed observational-memory observation cursor.",
    );
    requireValue(
      record.originType === "initial" || record.originType === "reflection",
      "Malformed observational-memory generation origin.",
    );
    for (const key of [
      "bufferedObservations",
      "bufferedReflection",
      "observedTimezone",
    ])
      requireValue(
        record[key] === undefined || typeof record[key] === "string",
        "Malformed observational-memory text.",
      );
    for (const key of [
      "bufferedObservationTokens",
      "bufferedReflectionTokens",
      "bufferedReflectionInputTokens",
      "reflectedObservationLineCount",
    ])
      requireValue(
        record[key] === undefined ||
          (typeof record[key] === "number" &&
            Number.isFinite(record[key]) &&
            record[key] >= 0),
        "Malformed observational-memory buffer counter.",
      );
    for (const key of ["observedMessageIds", "bufferedMessageIds"])
      requireValue(
        record[key] === undefined ||
          (Array.isArray(record[key]) &&
            record[key].every((id) => typeof id === "string")),
        "Malformed observational-memory message identities.",
      );
    const chunks = record.bufferedObservationChunks;
    requireValue(
      chunks === undefined || Array.isArray(chunks),
      "Malformed observation buffer.",
    );
    if (Array.isArray(chunks))
      for (const chunk of chunks) {
        requireValue(
          isRecord(chunk) &&
            typeof chunk.id === "string" &&
            typeof chunk.cycleId === "string" &&
            typeof chunk.observations === "string" &&
            chunk.createdAt instanceof Date &&
            chunk.lastObservedAt instanceof Date &&
            typeof chunk.tokenCount === "number" &&
            Number.isFinite(chunk.tokenCount) &&
            chunk.tokenCount >= 0 &&
            typeof chunk.messageTokens === "number" &&
            Number.isFinite(chunk.messageTokens) &&
            chunk.messageTokens >= 0 &&
            Array.isArray(chunk.messageIds) &&
            chunk.messageIds.every((id) => typeof id === "string"),
          "Malformed observation buffer chunk.",
        );
      }
  }
}

function validateConfiguration(
  configuration: unknown,
): asserts configuration is Record<string, unknown> {
  requireValue(isRecord(configuration), "Malformed resolved configuration.");
  function containsTransport(value: unknown): boolean {
    if (Array.isArray(value)) return value.some(containsTransport);
    if (!isRecord(value)) return false;
    return Object.entries(value).some(
      ([key, item]) =>
        /^(headers|abortsignal)$/i.test(key) || containsTransport(item),
    );
  }
  requireValue(
    !containsTransport(configuration),
    "Replay configuration contains transport metadata.",
  );
  for (const memory of [configuration.memoryConfig, configuration.memory]) {
    if (!isRecord(memory)) continue;
    requireValue(
      memory.semanticRecall === undefined || memory.semanticRecall === false,
      "Semantic recall is outside isolated memory replay scope.",
    );
    for (const key of ["workingMemory", "observationalMemory"]) {
      const feature = memory[key];
      if (isRecord(feature) && feature.enabled !== false)
        requireValue(
          feature.scope === "thread" ||
            (key === "observationalMemory" && feature.scope === undefined),
          "Only thread-scoped memory is replayable.",
        );
    }
  }
}

function normalizeReplayConfiguration(
  configuration: Record<string, unknown>,
): Record<string, unknown> {
  const normalized = { ...configuration };
  for (const key of ["memoryConfig", "memory"]) {
    const memory = normalized[key];
    if (
      !isRecord(memory) ||
      !isRecord(memory.observationalMemory) ||
      memory.observationalMemory.enabled === false ||
      memory.observationalMemory.scope !== undefined
    )
      continue;
    normalized[key] = {
      ...memory,
      observationalMemory: { ...memory.observationalMemory, scope: "thread" },
    };
  }
  return normalized;
}

function usesObservationalMemory(
  configuration: Record<string, unknown>,
): boolean {
  return [configuration.memoryConfig, configuration.memory].some((memory) => {
    if (!isRecord(memory)) return false;
    const feature = memory.observationalMemory;
    return (
      feature !== undefined &&
      feature !== false &&
      (!isRecord(feature) || feature.enabled !== false)
    );
  });
}

/** Require native context selectors to match the leased and captured memory. */
export function validateMemoryReplayContext(
  selector: Pick<MastraMemorySnapshot, "threadId" | "resourceId">,
  requestContext: Record<string, unknown>,
): void {
  requireValue(
    !Object.hasOwn(requestContext, MASTRA_AUTH_TOKEN_KEY),
    "Native authentication tokens are not replayable request context.",
  );
  validateMemoryReplaySelectors(selector, requestContext);
}

/** Validate selectors before a capture callback can omit middleware overrides. */
export function validateMemoryReplaySelectors(
  selector: Pick<MastraMemorySnapshot, "threadId" | "resourceId">,
  requestContext: Record<string, unknown>,
): void {
  for (const [key, expected] of [
    [MASTRA_THREAD_ID_KEY, selector.threadId],
    [MASTRA_RESOURCE_ID_KEY, selector.resourceId],
  ] as const) {
    const value = requestContext[key];
    requireValue(
      value === undefined ||
        value === null ||
        value === "" ||
        value === expected,
      "Request-context memory selectors differ from the captured selectors.",
    );
  }
}

/** Build safe diagnostic evidence even when complete replay prerequisites are unavailable. */
export function createMemoryReplayEnvelope(
  input: MastraMemoryReplayInput,
): MastraMemoryReplayEnvelope {
  const incomplete = (reason: string): MastraMemoryReplayEnvelope => ({
    version: 3,
    complete: false,
    reasons: [reason],
    invocationId: "",
    rawInput: null,
    initialSnapshot: null,
    configuration: null,
    requestContext: null,
    files: [],
    omTape: [],
  });
  try {
    validateMemorySnapshot(input.initialSnapshot);
    const envelope: MastraMemoryReplayEnvelope = {
      version: 3,
      complete: true,
      reasons: [],
      invocationId: input.invocationId,
      rawInput: encodeMemoryValue(input.rawInput),
      initialSnapshot: encodeMemoryValue(input.initialSnapshot),
      configuration: encodeMemoryValue(
        normalizeReplayConfiguration(input.configuration),
      ),
      requestContext: encodeMemoryValue(input.requestContext),
      files: input.files.map((file) => ({
        url: file.url,
        mediaType: file.mediaType,
        ...binary(file.bytes),
      })),
      omTape: input.omTape === undefined ? [] : input.omTape,
    };
    // The combined envelope, including encoded bytes and metadata, shares one budget.
    const converted = strictMastraReplayValue(
      envelope,
      "Mastra memory replay envelope",
    );
    decodeConvertedMemoryReplayEnvelope(converted);
    return converted as MastraMemoryReplayEnvelope;
  } catch (error) {
    return incomplete(
      error instanceof MemoryReplayError
        ? error.message
        : error instanceof Error && /exceeds maximum/.test(error.message)
          ? error.message
          : "Memory replay prerequisites could not be captured safely.",
    );
  }
}

/** Produce the immutable final envelope after recorded OM work has settled. */
export function finalizeMemoryReplayEnvelope(
  envelope: MastraMemoryReplayEnvelope,
  omTape: JsonValue[],
): MastraMemoryReplayEnvelope {
  const final = strictMastraReplayValue(
    { ...envelope, omTape },
    "Mastra memory replay envelope",
  ) as MastraMemoryReplayEnvelope;
  decodeConvertedMemoryReplayEnvelope(final);
  return final;
}

export function decodeMemoryReplayEnvelope(
  input: unknown,
): MastraMemoryReplayInput {
  const value = strictMastraReplayValue(input, "Mastra memory replay envelope");
  return decodeConvertedMemoryReplayEnvelope(value);
}

/** Validate a value already copied through the strict replay codec. */
function decodeConvertedMemoryReplayEnvelope(
  value: JsonValue,
): MastraMemoryReplayInput {
  requireValue(
    isRecord(value) &&
      (value.version === 2 || value.version === 3) &&
      value.complete === true &&
      Array.isArray(value.reasons) &&
      value.reasons.length === 0 &&
      typeof value.invocationId === "string" &&
      value.invocationId.length > 0 &&
      Array.isArray(value.files),
    "Missing, incomplete, or unknown version of memory replay envelope.",
  );
  requireValue(
    value.version === 2 || Array.isArray(value.omTape),
    "Malformed recorded observational-memory tape.",
  );
  for (const key of [
    "rawInput",
    "initialSnapshot",
    "configuration",
    "requestContext",
  ])
    requireValue(
      Object.hasOwn(value, key),
      "Missing memory replay prerequisite.",
    );
  const initialSnapshot = decodeMemoryValue(value.initialSnapshot as JsonValue);
  validateMemorySnapshot(initialSnapshot);
  const configuration = decodeMemoryValue(value.configuration as JsonValue);
  validateConfiguration(configuration);
  requireValue(
    value.version !== 2 || !usesObservationalMemory(configuration),
    "mastra_om_tape_missing",
  );
  const requestContext = decodeMemoryValue(value.requestContext as JsonValue);
  requireValue(isRecord(requestContext), "Malformed recorded request context.");
  validateMemoryReplayContext(initialSnapshot, requestContext);
  if (isRecord(configuration.runOptions)) {
    const memory = configuration.runOptions.memory;
    requireValue(isRecord(memory), "Missing invocation memory selectors.");
    const threadId = isRecord(memory.thread) ? memory.thread.id : memory.thread;
    requireValue(
      threadId === initialSnapshot.threadId &&
        memory.resource === initialSnapshot.resourceId,
      "Invocation memory selectors differ from the captured selectors.",
    );
  }
  const urls = new Set<string>();
  const files = value.files.map((file) => {
    requireValue(
      isRecord(file) &&
        typeof file.url === "string" &&
        !urls.has(file.url) &&
        typeof file.mediaType === "string" &&
        file.mediaType.length > 0,
      "Malformed or duplicate recorded file.",
    );
    validateUrl(file.url);
    const bytes = readBinary(file);
    if (value.version === 3)
      requireValue(
        /^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/.test(file.url) &&
          file.url === fileReference({ mediaType: file.mediaType, bytes }),
        "Recorded file must use its captured content reference.",
      );
    urls.add(file.url);
    return {
      url: file.url,
      mediaType: file.mediaType,
      bytes,
    };
  });
  return {
    invocationId: value.invocationId,
    rawInput: decodeMemoryValue(value.rawInput as JsonValue),
    initialSnapshot,
    configuration,
    requestContext,
    files,
    ...(value.version === 3 ? { omTape: value.omTape as JsonValue[] } : {}),
  };
}

/** Return undefined for legacy inputs; a present but invalid v2 envelope always rejects. */
export function restoreMemoryReplayEnvelope(
  input: unknown,
): MastraMemoryReplayInput | undefined {
  if (!isRecord(input) || !Object.hasOwn(input, MEMORY_REPLAY_KEY))
    return undefined;
  return decodeMemoryReplayEnvelope(input[MEMORY_REPLAY_KEY]);
}
