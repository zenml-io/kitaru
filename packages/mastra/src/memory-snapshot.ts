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
  boundMastraReplayEvidence,
  containsUrlCredentials,
  degradedMastraReplayEvidence,
  MAX_MASTRA_REPLAY_ITEMS,
  MAX_MASTRA_REPLAY_JSON_BYTES,
  MastraReplayBudgetError,
  type MastraReplayEvidence,
  type RecordingLimits,
  redactUrlCredentials,
  strictMastraReplayValue,
} from "@zenml-io/kitaru/adapter";
import {
  type AttachmentTokenCounts,
  readAttachmentTokenCounts,
} from "./attachment-tokens.js";
import {
  describeReplayFailure,
  getReplayReason,
  type MastraReplayReason,
  unsupportedMemoryReplay,
} from "./replay-reasons.js";
import { fileReference, MAX_CAPTURED_FILE_BYTES } from "./stateful-files.js";

export const MEMORY_REPLAY_KEY = "mastra_memory_replay";
const CODEC_KEY = "$mastra";
const FILE_REFERENCE = /^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/;

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
  /** The Kitaru blob that stores `bytes`, once one does. */
  blobId?: string;
}

/** A recorded file whose content is stored as a Kitaru blob. */
export interface MastraStoredFile {
  url: string;
  mediaType: string;
  blobId: string;
  length: number;
  sha256: string;
}

/** A recorded file with its content, or with the blob that stores it. */
export type MastraRecordedFileSource = MastraRecordedFile | MastraStoredFile;

/**
 * A recorded file in the envelope.
 *
 * An entry names the blob that stores the file's content. An entry without a
 * blob id belongs to a turn that has not stored its files yet and cannot be
 * replayed. Envelopes recorded before blob storage hold the content inline as
 * `base64`.
 */
export interface MastraFileManifestEntry {
  [key: string]: JsonValue;
  url: string;
  mediaType: string;
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
  files: MastraRecordedFileSource[];
  omTape?: JsonValue[];
  /**
   * When the recorded turn started; replay evaluates memory time checks at
   * this time. Defaults to the envelope's creation time and is absent only in
   * version-2 envelopes.
   */
  turnStartedAt?: Date;
  /** The tokens the recorded turn counted for each captured attachment. */
  attachmentTokens?: AttachmentTokenCounts;
}

/** How to restore each object's recorded key order after storage re-sorts keys. */
export interface MastraKeyOrder {
  [key: string]: JsonValue;
  /** Permutations of non-sorted objects, in sorted depth-first order. */
  permutations: string;
  /** SHA-256 of the envelope's JSON, without this field, in recorded order. */
  sha256: string;
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
  turnStartedAt: string;
  keyOrder: MastraKeyOrder;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireValue(
  condition: unknown,
  message: string,
  reason?: MastraReplayReason,
): asserts condition {
  if (!condition) throw unsupportedMemoryReplay(message, reason);
}

function requireStoredShape(
  condition: unknown,
  message: string,
): asserts condition {
  requireValue(condition, message, "memory_store_shape_unsupported");
}

function requireWithinBudget(
  condition: boolean,
  path: string | undefined,
  bound: string,
): void {
  if (!condition)
    throw new MastraReplayBudgetError(
      path === undefined
        ? `Unsupported Mastra memory replay: Memory value ${bound}.`
        : `${path} ${bound}`,
    );
}

function hash(data: Uint8Array | string): string {
  return createHash("sha256").update(data).digest("hex");
}

/** Read a string that is exactly a `Date#toISOString` rendering. */
function parseIsoDate(value: string): Date | undefined {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) && date.toISOString() === value
    ? date
    : undefined;
}

function binary(bytes: Uint8Array): {
  base64: string;
  length: number;
  sha256: string;
} {
  requireValue(
    bytes.byteLength <= 8 * 1_048_576,
    "Binary content exceeds maximum file bytes 8388608.",
    "replay_input_too_large",
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

const BLOB_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA256 = /^[a-f0-9]{64}$/;

function encodeFileEntry(
  file: MastraRecordedFileSource,
): MastraFileManifestEntry {
  const stored =
    "bytes" in file
      ? {
          blobId: file.blobId,
          length: file.bytes.byteLength,
          sha256: hash(file.bytes),
        }
      : file;
  requireValue(
    stored.length <= MAX_CAPTURED_FILE_BYTES,
    `Recorded file exceeds maximum file bytes ${MAX_CAPTURED_FILE_BYTES}.`,
    "replay_input_too_large",
  );
  return {
    url: file.url,
    mediaType: file.mediaType,
    ...(stored.blobId === undefined ? {} : { blobId: stored.blobId }),
    length: stored.length,
    sha256: stored.sha256,
  };
}

/**
 * Read a recorded file entry, or return undefined for an entry whose content
 * was not stored yet when `allowUnstored` is set.
 */
function readFileEntry(
  file: Record<string, unknown> & { url: string; mediaType: string },
  version: 2 | 3,
  allowUnstored: boolean,
): MastraRecordedFileSource | undefined {
  if (Object.hasOwn(file, "base64")) {
    const bytes = readBinary(file);
    if (version === 3)
      requireValue(
        FILE_REFERENCE.test(file.url) &&
          file.url === fileReference({ mediaType: file.mediaType, bytes }),
        "Recorded file must use its captured content reference.",
      );
    return { url: file.url, mediaType: file.mediaType, bytes };
  }
  requireValue(
    version === 3 &&
      FILE_REFERENCE.test(file.url) &&
      typeof file.length === "number" &&
      Number.isSafeInteger(file.length) &&
      file.length >= 0 &&
      file.length <= MAX_CAPTURED_FILE_BYTES &&
      typeof file.sha256 === "string" &&
      SHA256.test(file.sha256),
    "Malformed recorded file.",
  );
  if (!Object.hasOwn(file, "blobId")) {
    requireValue(allowUnstored, "Recorded file content was not stored.");
    return undefined;
  }
  requireValue(
    typeof file.blobId === "string" && BLOB_ID.test(file.blobId),
    "Malformed recorded file blob id.",
  );
  return {
    url: file.url,
    mediaType: file.mediaType,
    blobId: file.blobId,
    length: file.length,
    sha256: file.sha256,
  };
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
    url.protocol === "https:" || url.protocol === "http:",
    "Only captured file references and web URLs are replayable.",
  );
  requireValue(
    !containsUrlCredentials(url.href),
    "URL credentials are not replayable.",
  );
  return url;
}

/**
 * Encode the few non-JSON values in native memory without losing their types.
 *
 * URL credentials in strings and URL values are redacted, so a signed link in
 * thread history or model output never reaches recorded JSON. `path` names
 * the value in a budget error; other failures keep the replay codec's own
 * reasons.
 */
export function encodeMemoryValue(value: unknown, path?: string): JsonValue {
  let items = 0;
  const active = new Set<object>();
  function visit(current: unknown, depth: number): JsonValue {
    requireWithinBudget(
      ++items <= MAX_MASTRA_REPLAY_ITEMS,
      path,
      `exceeds maximum item count ${MAX_MASTRA_REPLAY_ITEMS}`,
    );
    requireWithinBudget(depth < 64, path, "exceeds maximum depth 64");
    if (current === undefined) return { [CODEC_KEY]: "undefined" };
    if (current === null || typeof current === "boolean") return current;
    if (typeof current === "string") {
      requireWithinBudget(
        current.length <= MAX_MASTRA_REPLAY_JSON_BYTES,
        path,
        `exceeds maximum JSON bytes ${MAX_MASTRA_REPLAY_JSON_BYTES}`,
      );
      return redactUrlCredentials(current);
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
      const href = redactUrlCredentials(current.href);
      validateUrl(href);
      return { [CODEC_KEY]: "url", value: href };
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
  return strictMastraReplayValue(encoded, path);
}

/**
 * Encode diagnostic evidence with the replay codec without failing on size.
 *
 * A value over the replay budget becomes a degraded marker that names the
 * exceeded bound, and optional per-value `limits` truncate the encoded value.
 * Values the codec cannot represent, or that sit under credential keys, still
 * throw.
 */
export function encodeMemoryEvidence(
  value: unknown,
  path: string,
  limits?: RecordingLimits,
): MastraReplayEvidence {
  let encoded: JsonValue;
  try {
    encoded = encodeMemoryValue(value, path);
  } catch (error) {
    if (error instanceof MastraReplayBudgetError)
      return degradedMastraReplayEvidence(path, error);
    throw error;
  }
  return limits === undefined
    ? { value: encoded }
    : boundMastraReplayEvidence(encoded, path, limits);
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
        const date = parseIsoDate(current.value);
        requireValue(date, "Malformed memory Date.");
        return date;
      }
      if (kind === "bytes" && Object.keys(current).length === 4)
        return readBinary(current);
      throw unsupportedMemoryReplay("Malformed memory codec tag.");
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

const KEY_ORDER_KEY = "keyOrder";
const PERMUTATIONS_PATTERN = /^(?:\d+:\d+(?:,\d+)*(?:;\d+:\d+(?:,\d+)*)*)?$/;

/**
 * Describe the key order of every object that is not already sorted.
 *
 * Entries follow a depth-first walk that visits keys in sorted order, so the
 * description does not depend on the order storage returns keys in. Each entry
 * is `gap:permutation`: the object's walk index minus the previous entry's,
 * then each recorded key's position in the sorted key list.
 */
function describeKeyOrder(value: JsonValue): string {
  const entries: string[] = [];
  let index = 0;
  let previous = 0;
  const visit = (current: JsonValue): void => {
    if (Array.isArray(current)) {
      for (const item of current) visit(item);
      return;
    }
    if (!isRecord(current)) return;
    const position = index++;
    const recorded = Object.keys(current);
    const sorted = [...recorded].sort();
    if (recorded.some((key, i) => key !== sorted[i])) {
      const rank = new Map(sorted.map((key, i) => [key, i]));
      entries.push(
        `${position - previous}:${recorded.map((key) => rank.get(key)).join(",")}`,
      );
      previous = position;
    }
    for (const key of sorted) visit(current[key] as JsonValue);
  };
  visit(value);
  return entries.join(";");
}

/** Rebuild every object of `value` in the key order `permutations` describes. */
function applyKeyOrder(value: JsonValue, permutations: string): JsonValue {
  requireValue(
    PERMUTATIONS_PATTERN.test(permutations),
    "Malformed recorded key order.",
  );
  const pending = new Map<number, number[]>();
  let position = 0;
  for (const entry of permutations ? permutations.split(";") : []) {
    const [gap, order] = entry.split(":") as [string, string];
    position += Number(gap);
    requireValue(!pending.has(position), "Malformed recorded key order.");
    pending.set(position, order.split(",").map(Number));
  }
  let index = 0;
  const visit = (current: JsonValue): JsonValue => {
    if (Array.isArray(current)) return current.map(visit);
    if (!isRecord(current)) return current;
    const own = index++;
    const sorted = Object.keys(current).sort();
    const children = new Map(
      sorted.map((key) => [key, visit(current[key] as JsonValue)]),
    );
    const permutation = pending.get(own);
    pending.delete(own);
    requireValue(
      permutation === undefined ||
        (permutation.length === sorted.length &&
          new Set(permutation).size === sorted.length &&
          permutation.every((rank) => rank < sorted.length)),
      "Recorded key order does not match the stored envelope.",
    );
    const order = permutation?.map((rank) => sorted[rank] as string) ?? sorted;
    return Object.fromEntries(
      order.map((key) => [key, children.get(key) as JsonValue]),
    );
  };
  const restored = visit(value);
  requireValue(
    pending.size === 0,
    "Recorded key order does not match the stored envelope.",
  );
  return restored;
}

/**
 * Add the key order that restores an envelope byte for byte after storage.
 *
 * PostgreSQL `jsonb` re-sorts object keys, and replay sends the restored
 * values to the provider, so tool arguments, tool results, schemas, and
 * working-memory templates would otherwise reach the model in a different
 * order than production sent them.
 */
function withKeyOrder(envelope: JsonValue): MastraMemoryReplayEnvelope {
  requireValue(isRecord(envelope), "Malformed memory replay envelope.");
  const { [KEY_ORDER_KEY]: _previous, ...content } = envelope;
  const json = JSON.stringify(content);
  const keyOrder: MastraKeyOrder = {
    permutations: describeKeyOrder(content),
    sha256: hash(json),
  };
  const bytes =
    Buffer.byteLength(json, "utf8") +
    Buffer.byteLength(JSON.stringify({ [KEY_ORDER_KEY]: keyOrder }), "utf8");
  if (bytes > MAX_MASTRA_REPLAY_JSON_BYTES)
    throw new MastraReplayBudgetError(
      `Mastra memory replay envelope exceeds maximum JSON bytes ${MAX_MASTRA_REPLAY_JSON_BYTES}`,
    );
  return {
    ...content,
    [KEY_ORDER_KEY]: keyOrder,
  } as MastraMemoryReplayEnvelope;
}

/**
 * Restore a stored envelope's recorded key order.
 *
 * `verify` then confirms the restored envelope matches the recorded one byte
 * for byte; decoding runs it last so malformed content reports its own reason.
 */
function restoreKeyOrder(envelope: Record<string, JsonValue>): {
  restored: Record<string, JsonValue>;
  verify(): void;
} {
  const keyOrder = envelope[KEY_ORDER_KEY];
  requireValue(
    isRecord(keyOrder) &&
      typeof keyOrder.permutations === "string" &&
      typeof keyOrder.sha256 === "string",
    "Missing recorded key order.",
  );
  const { [KEY_ORDER_KEY]: _keyOrder, ...content } = envelope;
  const restored = applyKeyOrder(content, keyOrder.permutations) as Record<
    string,
    JsonValue
  >;
  return {
    restored,
    verify: () =>
      requireValue(
        hash(JSON.stringify(restored)) === keyOrder.sha256,
        "Restored envelope differs from the recorded envelope.",
      ),
  };
}

function readStoredDates(value: unknown, keys: readonly string[]): unknown {
  if (!isRecord(value)) return value;
  const copy = { ...value };
  for (const key of keys) {
    const stored = copy[key];
    if (typeof stored !== "string") continue;
    // Only an exact ISO rendering is a serialized Date; other strings stay
    // unchanged so validation still rejects them.
    const date = parseIsoDate(stored);
    if (date) copy[key] = date;
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
  requireStoredShape(
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
  requireStoredShape(
    value.thread === null ||
      (isRecord(value.thread) &&
        value.thread.id === value.threadId &&
        value.thread.resourceId === value.resourceId &&
        dates(value.thread)),
    "Malformed or mismatched thread record.",
  );
  requireStoredShape(
    value.resource === null ||
      (isRecord(value.resource) &&
        value.resource.id === value.resourceId &&
        dates(value.resource)),
    "Malformed or mismatched resource record.",
  );
  requireStoredShape(
    value.thread !== null ||
      (value.messages.length === 0 && value.records.length === 0),
    "Orphaned memory state.",
  );
  const messageIds = new Set<string>();
  for (const message of value.messages) {
    requireStoredShape(
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
    requireStoredShape(
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
      requireStoredShape(
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
        "om_work_unjoined",
      );
    requireStoredShape(
      record.lastBufferedAtTime === null ||
        record.lastBufferedAtTime instanceof Date,
      "Malformed observational-memory buffer cursor.",
    );
    requireStoredShape(
      record.lastObservedAt === undefined ||
        record.lastObservedAt instanceof Date,
      "Malformed observational-memory observation cursor.",
    );
    requireStoredShape(
      record.originType === "initial" || record.originType === "reflection",
      "Malformed observational-memory generation origin.",
    );
    for (const key of [
      "bufferedObservations",
      "bufferedReflection",
      "observedTimezone",
    ])
      requireStoredShape(
        record[key] === undefined || typeof record[key] === "string",
        "Malformed observational-memory text.",
      );
    for (const key of [
      "bufferedObservationTokens",
      "bufferedReflectionTokens",
      "bufferedReflectionInputTokens",
      "reflectedObservationLineCount",
    ])
      requireStoredShape(
        record[key] === undefined ||
          (typeof record[key] === "number" &&
            Number.isFinite(record[key]) &&
            record[key] >= 0),
        "Malformed observational-memory buffer counter.",
      );
    for (const key of ["observedMessageIds", "bufferedMessageIds"])
      requireStoredShape(
        record[key] === undefined ||
          (Array.isArray(record[key]) &&
            record[key].every((id) => typeof id === "string")),
        "Malformed observational-memory message identities.",
      );
    const chunks = record.bufferedObservationChunks;
    requireStoredShape(
      chunks === undefined || Array.isArray(chunks),
      "Malformed observation buffer.",
    );
    if (Array.isArray(chunks))
      for (const chunk of chunks) {
        requireStoredShape(
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
  requireValue(
    isRecord(configuration),
    "Malformed resolved configuration.",
    "memory_config_unsupported",
  );
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
    "credential_key_unsupported",
  );
  for (const memory of [configuration.memoryConfig, configuration.memory]) {
    if (!isRecord(memory)) continue;
    requireValue(
      memory.semanticRecall === undefined || memory.semanticRecall === false,
      "Semantic recall is outside isolated memory replay scope.",
      "memory_config_unsupported",
    );
    for (const key of ["workingMemory", "observationalMemory"]) {
      const feature = memory[key];
      if (isRecord(feature) && feature.enabled !== false)
        requireValue(
          feature.scope === "thread" ||
            (key === "observationalMemory" && feature.scope === undefined),
          "Only thread-scoped memory is replayable.",
          "memory_config_unsupported",
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
    "credential_key_unsupported",
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
      "context_unsupported",
    );
  }
}

/**
 * Rewrites an encoded envelope before its key order and hash are recorded.
 *
 * Recording the order after the rewrite keeps the hash valid for the content
 * that reaches storage.
 */
export type MemoryReplayEnvelopeSanitizer = (value: JsonValue) => JsonValue;

/** A replay envelope and, when it is incomplete, why. */
export interface MemoryReplayEnvelopeCapture {
  envelope: MastraMemoryReplayEnvelope;
  reason?: MastraReplayReason;
}

/**
 * Record why replay prerequisites are unavailable, without any of them.
 *
 * `message` is stored on the server and must not contain recorded data.
 */
export function createIncompleteMemoryReplayEnvelope(
  message: string,
): MastraMemoryReplayEnvelope {
  return {
    version: 3,
    complete: false,
    reasons: [message],
    invocationId: "",
    rawInput: null,
    initialSnapshot: null,
    configuration: null,
    requestContext: null,
    files: [],
    omTape: [],
    turnStartedAt: "",
    keyOrder: { permutations: "", sha256: "" },
  };
}

/**
 * Build safe diagnostic evidence even when complete replay prerequisites are unavailable.
 *
 * `sanitize` receives the encoded envelope before its key order is recorded.
 */
export function createMemoryReplayEnvelope(
  input: MastraMemoryReplayInput,
  sanitize: MemoryReplayEnvelopeSanitizer = (value) => value,
): MastraMemoryReplayEnvelope {
  return captureMemoryReplayEnvelope(input, sanitize).envelope;
}

/**
 * Build a replay envelope and name the reason code when it is incomplete.
 *
 * `sanitize` receives the encoded envelope before its key order is recorded.
 */
export function captureMemoryReplayEnvelope(
  input: MastraMemoryReplayInput,
  sanitize: MemoryReplayEnvelopeSanitizer = (value) => value,
): MemoryReplayEnvelopeCapture {
  try {
    validateMemorySnapshot(input.initialSnapshot);
    const turnStartedAt = input.turnStartedAt ?? new Date();
    requireValue(
      Number.isFinite(turnStartedAt.getTime()),
      "Invalid recorded turn start time.",
    );
    const envelope: Omit<MastraMemoryReplayEnvelope, "keyOrder"> = {
      version: 3,
      complete: true,
      reasons: [],
      invocationId: input.invocationId,
      rawInput: encodeMemoryValue(input.rawInput, "Invocation input"),
      initialSnapshot: encodeMemoryValue(
        input.initialSnapshot,
        "Initial memory snapshot",
      ),
      configuration: encodeMemoryValue(
        normalizeReplayConfiguration(input.configuration),
        "Replay configuration",
      ),
      requestContext: encodeMemoryValue(
        input.requestContext,
        "Request context",
      ),
      files: input.files.map(encodeFileEntry),
      omTape: input.omTape === undefined ? [] : input.omTape,
      turnStartedAt: turnStartedAt.toISOString(),
    };
    // The combined envelope, including encoded bytes and metadata, shares one budget.
    const converted = withKeyOrder(
      sanitize(
        strictMastraReplayValue(envelope, "Mastra memory replay envelope"),
      ),
    );
    // Files are stored as blobs once the turn has finished.
    decodeConvertedMemoryReplayEnvelope(converted, true);
    return { envelope: converted };
  } catch (error) {
    return {
      envelope: createIncompleteMemoryReplayEnvelope(
        describeReplayFailure(
          error,
          "Memory replay prerequisites could not be captured safely.",
        ),
      ),
      reason: getReplayReason(error, "capture_prerequisite_failed"),
    };
  }
}

/**
 * Produce the immutable final envelope after recorded OM work has settled.
 *
 * `sanitize` receives the encoded envelope before its key order is recorded.
 */
export function finalizeMemoryReplayEnvelope(
  envelope: MastraMemoryReplayEnvelope,
  omTape: JsonValue[],
  sanitize: MemoryReplayEnvelopeSanitizer = (value) => value,
  attachmentTokens: AttachmentTokenCounts = {},
  allowUnstoredFiles = false,
): MastraMemoryReplayEnvelope {
  const final = withKeyOrder(
    sanitize(
      strictMastraReplayValue(
        {
          ...envelope,
          omTape,
          ...(Object.keys(attachmentTokens).length > 0
            ? { attachmentTokens }
            : {}),
        },
        "Mastra memory replay envelope",
      ),
    ),
  );
  decodeConvertedMemoryReplayEnvelope(final, allowUnstoredFiles);
  return final;
}

export function decodeMemoryReplayEnvelope(
  input: unknown,
): MastraMemoryReplayInput {
  const value = strictMastraReplayValue(input, "Mastra memory replay envelope");
  return decodeConvertedMemoryReplayEnvelope(value);
}

/**
 * Validate a value already copied through the strict replay codec.
 *
 * With `allowUnstoredFiles`, file entries without stored content pass
 * validation and are left out of the result.
 */
function decodeConvertedMemoryReplayEnvelope(
  stored: JsonValue,
  allowUnstoredFiles = false,
): MastraMemoryReplayInput {
  requireValue(
    isRecord(stored) && (stored.version === 2 || stored.version === 3),
    "Missing, incomplete, or unknown version of memory replay envelope.",
  );
  const ordered = stored.version === 3 ? restoreKeyOrder(stored) : undefined;
  const value = ordered?.restored ?? stored;
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
  const turnStartedAt =
    value.version === 3 && typeof value.turnStartedAt === "string"
      ? parseIsoDate(value.turnStartedAt)
      : undefined;
  requireValue(
    value.version === 2 || turnStartedAt !== undefined,
    "Missing or malformed recorded turn start time.",
  );
  const attachmentTokens =
    value.version === 3
      ? readAttachmentTokenCounts(value.attachmentTokens as JsonValue)
      : undefined;
  requireValue(
    !Object.hasOwn(value, "attachmentTokens") || attachmentTokens !== undefined,
    "Malformed recorded attachment token counts.",
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
  const files: MastraRecordedFileSource[] = [];
  for (const file of value.files) {
    requireValue(
      isRecord(file) &&
        typeof file.url === "string" &&
        !urls.has(file.url) &&
        typeof file.mediaType === "string" &&
        file.mediaType.length > 0,
      "Malformed or duplicate recorded file.",
    );
    validateUrl(file.url);
    urls.add(file.url);
    const entry = readFileEntry(
      file as Record<string, unknown> & { url: string; mediaType: string },
      value.version,
      allowUnstoredFiles,
    );
    if (entry) files.push(entry);
  }
  const rawInput = decodeMemoryValue(value.rawInput as JsonValue);
  ordered?.verify();
  return {
    invocationId: value.invocationId,
    rawInput,
    initialSnapshot,
    configuration,
    requestContext,
    files,
    ...(value.version === 3
      ? { omTape: value.omTape as JsonValue[], turnStartedAt }
      : {}),
    ...(attachmentTokens ? { attachmentTokens } : {}),
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
