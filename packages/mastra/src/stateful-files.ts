import { createHash } from "node:crypto";
import { redactUrlCredentials } from "@zenml-io/kitaru/adapter";
import type {
  MastraRecordedFile,
  MastraRecordedFileSource,
} from "./memory-snapshot.js";
import { MastraReplayReasonError } from "./replay-reasons.js";

export interface ResolvedMemoryFile {
  bytes: Uint8Array;
  mediaType: string;
}

export type MemoryFileResolver = (url: string) => Promise<ResolvedMemoryFile>;

export interface RecordedEvidenceSanitizer {
  replace<T>(value: T): T;
}

const FILE_REFERENCE = /^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/;
/** The most file bytes one turn captures, for one file or all of them together. */
export const MAX_CAPTURED_FILE_BYTES = 16 * 1024 * 1024;
/** The most distinct files one turn captures. */
export const MAX_RECORDED_FILES = 64;
const ABSOLUTE_URL = /^[a-z][a-z0-9+.-]*:\/\//i;

/** Return the WHATWG form of an absolute URL, or `value` when it is not one. */
function normalizeFileUrl(value: string): string {
  if (!ABSOLUTE_URL.test(value)) return value;
  try {
    return new URL(value).href;
  } catch {
    return value;
  }
}

/** Look a string up among declared file URLs, in the form the app wrote or its WHATWG form. */
function createFileUrlLookup(
  references: ReadonlyMap<string, string>,
): (value: string) => string | undefined {
  return (value) =>
    references.size === 0
      ? undefined
      : (references.get(value) ?? references.get(normalizeFileUrl(value)));
}

export function fileReference(file: ResolvedMemoryFile): string {
  const digest = createHash("sha256")
    .update(file.mediaType)
    .update("\0")
    .update(file.bytes)
    .digest("hex");
  return `kitaru-file://sha256/${digest}`;
}

/** How a file or image part held its inline content, so it can be written back exactly. */
export type InlineFileForm =
  | { encoding: "base64" }
  | { encoding: "bytes" }
  | { encoding: "data-url"; prefix: string };

/**
 * A file's content held inline by a stored message, recorded as the captured
 * file's reference and the form the message held it in.
 *
 * Replay writes the file's bytes back in that form before it restores the
 * message, so the replayed history is identical to the recorded one.
 */
export class InlineFileContent {
  constructor(
    readonly reference: string,
    readonly form: InlineFileForm,
  ) {}
}

/** Content a file or image part holds inline, with its captured reference. */
export interface InlineFile {
  reference: string;
  mediaType: string;
  bytes: Uint8Array;
  /** The form to write `bytes` back in, when that reproduces the part exactly. */
  form?: InlineFileForm;
}

const URL_SCHEME = /^[a-z][a-z0-9+.-]*:/i;

function getInlineData(part: Record<string, unknown>): unknown {
  return part.type === "image" ? part.image : part.data;
}

function getPartMediaType(part: Record<string, unknown>): string {
  const mediaType = [part.mediaType, part.mimeType].find(
    (value) => typeof value === "string",
  );
  return typeof mediaType === "string" ? mediaType : "";
}

/** Decode inline data, or return undefined for data that is a URL. */
function decodeInlineData(
  data: unknown,
): { bytes: Uint8Array; form?: InlineFileForm } | undefined {
  if (data instanceof Uint8Array)
    return { bytes: data, form: { encoding: "bytes" } };
  if (typeof data !== "string") return undefined;
  if (data.startsWith("data:")) {
    const comma = data.indexOf(",");
    const prefix = data.slice(0, comma + 1);
    const body = data.slice(comma + 1);
    const bytes = Buffer.from(body, "base64");
    return {
      bytes,
      form:
        comma > 0 &&
        prefix.endsWith(";base64,") &&
        bytes.toString("base64") === body
          ? { encoding: "data-url", prefix }
          : undefined,
    };
  }
  if (URL_SCHEME.test(data)) return undefined;
  const bytes = Buffer.from(data, "base64");
  return {
    bytes,
    form:
      bytes.toString("base64") === data ? { encoding: "base64" } : undefined,
  };
}

/** Write `bytes` in the inline form a part held them in. */
function encodeInlineData(
  bytes: Uint8Array,
  form: InlineFileForm,
): string | Uint8Array {
  if (form.encoding === "bytes") return new Uint8Array(bytes);
  const base64 = Buffer.from(bytes).toString("base64");
  return form.encoding === "data-url" ? `${form.prefix}${base64}` : base64;
}

/**
 * Return the content reference of an attachment a file or image part holds
 * inline, as bytes, base64 text, or a data URL, and undefined for one it
 * holds as a URL.
 *
 * `cache` keeps each inline value's reference, because Mastra hands the same
 * parts over on every step and hashing a large file each time is slow.
 */
export function getInlineFileReference(
  part: Record<string, unknown>,
  cache: Map<unknown, string>,
): string | undefined {
  const data = getInlineData(part);
  const cached = cache.get(data);
  if (cached) return cached;
  const decoded = decodeInlineData(data);
  if (!decoded) return undefined;
  const reference = fileReference({
    bytes: decoded.bytes,
    mediaType: getPartMediaType(part),
  });
  cache.set(data, reference);
  return reference;
}

/**
 * Create a reader of the inline content of file and image parts.
 *
 * The reader decodes and hashes each distinct value once per media type, so
 * a turn that meets the same large attachment in its history, its evidence
 * and its observational-memory inputs hashes it only once.
 */
export function createInlineFileReader(): ((
  part: Record<string, unknown>,
) => InlineFile | undefined) & {
  /** The file read with this content reference, if any. */
  byReference(reference: string): InlineFile | undefined;
} {
  const cache = new Map<string, Map<unknown, InlineFile | null>>();
  const references = new Map<string, InlineFile>();
  const read = (part: Record<string, unknown>): InlineFile | undefined => {
    const data = getInlineData(part);
    const mediaType = getPartMediaType(part);
    let byData = cache.get(mediaType);
    if (!byData) {
      byData = new Map();
      cache.set(mediaType, byData);
    }
    const cached = byData.get(data);
    if (cached !== undefined) return cached ?? undefined;
    const decoded = decodeInlineData(data);
    const file = decoded
      ? {
          reference: fileReference({ bytes: decoded.bytes, mediaType }),
          mediaType,
          bytes: decoded.bytes,
          ...(decoded.form ? { form: decoded.form } : {}),
        }
      : null;
    byData.set(data, file);
    if (file) references.set(file.reference, file);
    return file ?? undefined;
  };
  return Object.assign(read, {
    byReference: (reference: string) => references.get(reference),
  });
}

export type InlineFileReader = ReturnType<typeof createInlineFileReader>;

/**
 * Replace inline file content in thread history with `InlineFileContent`
 * wherever it matches a known file, leaving `snapshot` itself unchanged.
 *
 * `files` are the files the replay input already records. A known file not
 * among them joins the returned `files` while the capture limits allow it;
 * past them, and wherever the recorded form could not be written back
 * exactly, the content stays inline.
 */
export function referenceInlineFiles<T extends { messages: unknown[] }>(
  snapshot: T,
  options: {
    read: InlineFileReader;
    isKnown: (reference: string) => boolean;
    files: readonly MastraRecordedFileSource[];
  },
): { snapshot: T; files: MastraRecordedFile[] } {
  const recorded = new Set(options.files.map((file) => file.url));
  let totalBytes = options.files.reduce(
    (size, file) =>
      size + ("bytes" in file ? file.bytes.byteLength : file.length),
    0,
  );
  const added: MastraRecordedFile[] = [];
  function toReference(
    part: Record<string, unknown>,
  ): InlineFileContent | string | Uint8Array | undefined {
    const data = getInlineData(part);
    // Content an earlier pass already referenced, before the limits below.
    const referenced = data instanceof InlineFileContent ? data : undefined;
    const file = referenced
      ? options.read.byReference(referenced.reference)
      : options.read(part);
    const form = referenced?.form ?? file?.form;
    if (!file || !form || !options.isKnown(file.reference)) {
      if (referenced)
        throw new MastraReplayReasonError(
          "Referenced inline file content was not read in this turn.",
          "memory_store_shape_unsupported",
        );
      return undefined;
    }
    if (!recorded.has(file.reference)) {
      if (
        recorded.size >= MAX_RECORDED_FILES ||
        totalBytes + file.bytes.byteLength > MAX_CAPTURED_FILE_BYTES
      )
        return referenced ? encodeInlineData(file.bytes, form) : undefined;
      recorded.add(file.reference);
      totalBytes += file.bytes.byteLength;
      added.push({
        url: file.reference,
        mediaType: file.mediaType,
        bytes: file.bytes,
      });
    }
    return referenced ?? new InlineFileContent(file.reference, form);
  }
  const visit = createFilePartVisitor((part) => {
    const content = toReference(part);
    return content === undefined
      ? part
      : { ...part, [part.type === "image" ? "image" : "data"]: content };
  });
  const messages = visit(snapshot.messages) as unknown[];
  return {
    snapshot:
      messages === snapshot.messages ? snapshot : { ...snapshot, messages },
    files: added,
  };
}

/**
 * Build a function that replaces the inline content of each file or image
 * part holding a known file with `InlineFileContent`, leaving `value` itself
 * unchanged.
 *
 * It keeps content that could not be written back exactly inline, as
 * `referenceInlineFiles` does, and leaves the capture limits to that later
 * pass, so later copies and checks of a large history skip its bytes.
 */
export function createInlineContentReferencer(
  read: InlineFileReader,
  isKnown: (reference: string) => boolean,
): <T>(value: T) => T {
  const visit = createFilePartVisitor((part) => {
    const file = read(part);
    if (!file?.form || !isKnown(file.reference)) return part;
    return {
      ...part,
      [part.type === "image" ? "image" : "data"]: new InlineFileContent(
        file.reference,
        file.form,
      ),
    };
  });
  return <T>(value: T) => visit(value) as T;
}

/**
 * Write each `InlineFileContent` in `value` back as the content it replaced.
 *
 * Throws when a referenced file was not recorded, so replay never restores a
 * message with content that differs from the recorded one.
 */
export function restoreInlineFiles<T>(
  value: T,
  readFile: (reference: string) => ResolvedMemoryFile | undefined,
): T {
  function visit(current: unknown): unknown {
    if (current instanceof InlineFileContent) {
      const file = readFile(current.reference);
      if (!file)
        throw new MastraReplayReasonError(
          "Unsupported Mastra memory replay: inline file content was not recorded.",
          "memory_store_shape_unsupported",
        );
      return encodeInlineData(file.bytes, current.form);
    }
    if (Array.isArray(current)) {
      const items = current.map(visit);
      return items.some((item, index) => item !== current[index])
        ? items
        : current;
    }
    if (!isPlainRecord(current)) return current;
    let changed = false;
    const entries = Object.entries(current).map(([key, item]) => {
      const next = visit(item);
      if (next !== item) changed = true;
      return [key, next] as const;
    });
    return changed ? Object.fromEntries(entries) : current;
  }
  return visit(value) as T;
}

/** The references of every `InlineFileContent` in `value`. */
export function collectInlineFileReferences(value: unknown): string[] {
  const references: string[] = [];
  function visit(current: unknown): void {
    if (current instanceof InlineFileContent) {
      references.push(current.reference);
      return;
    }
    if (Array.isArray(current)) for (const item of current) visit(item);
    else if (isPlainRecord(current))
      for (const item of Object.values(current)) visit(item);
  }
  visit(value);
  return references;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype ||
      Object.getPrototypeOf(value) === null)
  );
}

/**
 * Build a copy-on-write walk over arrays and plain objects that hands each
 * file or image part to `replacePart` and does not descend into it.
 */
function createFilePartVisitor(
  replacePart: (part: Record<string, unknown>) => Record<string, unknown>,
): (value: unknown) => unknown {
  function visit(current: unknown): unknown {
    if (Array.isArray(current)) {
      const items = current.map(visit);
      return items.some((item, index) => item !== current[index])
        ? items
        : current;
    }
    if (!isPlainRecord(current)) return current;
    if (isFilePart(current)) return replacePart(current);
    let changed = false;
    const entries = Object.entries(current).map(([key, item]) => {
      const next = visit(item);
      if (next !== item) changed = true;
      return [key, next] as const;
    });
    return changed ? Object.fromEntries(entries) : current;
  }
  return visit;
}

/**
 * Build a function that replaces the inline content of each file or image
 * part holding a captured file with that file's reference, leaving `value`
 * itself unchanged.
 *
 * The recorded file already holds the bytes, so evidence that repeats them
 * on every model step only grows the recording.
 */
export function createCapturedContentReferencer(
  isCaptured: (reference: string) => boolean,
  read: InlineFileReader = createInlineFileReader(),
): <T>(value: T) => T {
  const visit = createFilePartVisitor((part) => {
    const reference = read(part)?.reference;
    if (!reference || !isCaptured(reference)) return part;
    return { ...part, [part.type === "image" ? "image" : "data"]: reference };
  });
  return <T>(value: T) => visit(value) as T;
}

function copiedFile(file: MastraRecordedFile): MastraRecordedFile {
  if (
    !FILE_REFERENCE.test(file.url) ||
    !(file.bytes instanceof Uint8Array) ||
    file.bytes.byteLength > MAX_CAPTURED_FILE_BYTES ||
    typeof file.mediaType !== "string" ||
    !file.mediaType ||
    fileReference(file) !== file.url
  )
    throw new Error("Unsupported Mastra memory replay: invalid recorded file.");
  return { ...file, bytes: new Uint8Array(file.bytes) };
}

/** Replay resolves only captured content references, never network URLs. */
export function restoreCapturedFiles(recorded: readonly MastraRecordedFile[]) {
  if (
    recorded.length > MAX_RECORDED_FILES ||
    recorded.reduce(
      (size, file) =>
        size + (file.bytes instanceof Uint8Array ? file.bytes.byteLength : 0),
      0,
    ) > MAX_CAPTURED_FILE_BYTES
  )
    throw new Error(
      "Unsupported Mastra memory replay: file capture limit exceeded.",
    );
  const files = recorded.map(copiedFile);
  const lookup = new Map(files.map((file) => [file.url, file]));
  if (lookup.size !== files.length)
    throw new Error(
      "Unsupported Mastra memory replay: duplicate recorded file.",
    );
  return {
    files,
    /** Whether a file with this content reference was recorded. */
    hasFile: (reference: string): boolean => lookup.has(reference),
    /** Return a copy of the recorded file with this reference, if any. */
    readFile: (reference: string): ResolvedMemoryFile | undefined => {
      const file = lookup.get(reference);
      return file
        ? { bytes: new Uint8Array(file.bytes), mediaType: file.mediaType }
        : undefined;
    },
    resolveFile: async (reference: string): Promise<ResolvedMemoryFile> => {
      const file = lookup.get(reference);
      if (!file)
        throw new Error(
          "Unsupported Mastra memory replay: file reference was not recorded.",
        );
      return { bytes: new Uint8Array(file.bytes), mediaType: file.mediaType };
    },
  };
}

const MAX_REMEMBERED_THREADS = 1024;

/**
 * Remember which files each thread's turns captured and stored, so a later
 * turn in this process recognizes their content when a stored message holds
 * it inline.
 *
 * Only the most recent threads, and each thread's most recent files up to
 * the per-turn file limit, are kept.
 */
export function createThreadFileRegistry() {
  const threads = new Map<string, Set<string>>();
  const key = (selector: { threadId: string; resourceId: string }) =>
    JSON.stringify([selector.resourceId, selector.threadId]);
  return {
    /** Whether a turn of this thread stored the file with `reference`. */
    has(
      selector: { threadId: string; resourceId: string },
      reference: string,
    ): boolean {
      return threads.get(key(selector))?.has(reference) ?? false;
    },
    remember(
      selector: { threadId: string; resourceId: string },
      references: readonly string[],
    ): void {
      const id = key(selector);
      const known = threads.get(id) ?? new Set<string>();
      threads.delete(id);
      threads.set(id, known);
      for (const reference of references) {
        known.delete(reference);
        known.add(reference);
      }
      while (known.size > MAX_RECORDED_FILES)
        known.delete(known.values().next().value as string);
      if (threads.size > MAX_REMEMBERED_THREADS)
        threads.delete(threads.keys().next().value as string);
    },
  };
}

/** Kitaru blob metadata, as the blob API returns it. */
export interface StoredBlob {
  id: string;
  sha256: string;
  size: number;
}

/** The Kitaru blob API calls that store and read captured files. */
export interface FileBlobClient {
  upload(
    content: Uint8Array,
    options: { filename?: string; mediaType?: string },
  ): Promise<StoredBlob>;
  get(blobId: string): Promise<StoredBlob>;
  download(blobId: string): Promise<Uint8Array>;
}

const MAX_REMEMBERED_BLOBS = 1024;

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * Store captured files as Kitaru blobs, uploading each file's content once.
 *
 * The server keeps one blob per content and media type. This store also
 * remembers the blob id of every file it stored, so a later turn that captures
 * the same file checks that the blob still exists instead of sending its bytes
 * again.
 */
export function createFileBlobStore(blobs: FileBlobClient) {
  const remembered = new Map<string, string>();
  function remember(reference: string, blobId: string): void {
    remembered.delete(reference);
    remembered.set(reference, blobId);
    if (remembered.size > MAX_REMEMBERED_BLOBS)
      remembered.delete(remembered.keys().next().value as string);
  }
  async function storeFile(file: MastraRecordedFile): Promise<string> {
    const digest = sha256Hex(file.bytes);
    const matches = (blob: StoredBlob) =>
      blob.sha256 === digest && blob.size === file.bytes.byteLength;
    const known = remembered.get(file.url);
    if (known !== undefined) {
      // A deleted blob is uploaded again instead of being referenced.
      const blob = await blobs.get(known).catch(() => undefined);
      if (blob && matches(blob)) return known;
      remembered.delete(file.url);
    }
    const blob = await blobs.upload(file.bytes, {
      filename: "mastra-file",
      mediaType: file.mediaType,
    });
    if (!matches(blob))
      throw new Error("The stored blob does not match the captured file.");
    remember(file.url, blob.id);
    return blob.id;
  }
  return {
    /**
     * Return `files` with the blob id each one is stored under, uploading
     * files that no blob holds yet.
     */
    async store(
      files: readonly MastraRecordedFile[],
    ): Promise<MastraRecordedFile[]> {
      try {
        return await Promise.all(
          files.map(async (file) =>
            file.blobId === undefined
              ? { ...file, blobId: await storeFile(file) }
              : file,
          ),
        );
      } catch {
        throw new MastraReplayReasonError(
          "Captured files could not be stored on the Kitaru server.",
          "file_store_failed",
        );
      }
    },
  };
}

/**
 * Read the content of recorded files, downloading the ones stored as blobs.
 *
 * Every downloaded file must match its recorded length, SHA-256 and content
 * reference; otherwise replay fails instead of using different content.
 */
export async function loadRecordedFiles(
  sources: readonly MastraRecordedFileSource[],
  blobs: Pick<FileBlobClient, "download">,
): Promise<MastraRecordedFile[]> {
  const declaredBytes = sources.reduce(
    (size, file) =>
      size + ("bytes" in file ? file.bytes.byteLength : file.length),
    0,
  );
  if (
    sources.length > MAX_RECORDED_FILES ||
    declaredBytes > MAX_CAPTURED_FILE_BYTES
  )
    throw new Error(
      "Unsupported Mastra memory replay: file capture limit exceeded.",
    );
  return Promise.all(
    sources.map(async (file): Promise<MastraRecordedFile> => {
      if ("bytes" in file) return file;
      let bytes: Uint8Array;
      try {
        bytes = await blobs.download(file.blobId);
      } catch {
        throw new Error(
          "Unsupported Mastra memory replay: recorded file content could not be downloaded.",
        );
      }
      if (
        bytes.byteLength !== file.length ||
        sha256Hex(bytes) !== file.sha256 ||
        fileReference({ bytes, mediaType: file.mediaType }) !== file.url
      )
        throw new Error(
          "Unsupported Mastra memory replay: recorded file content does not match its reference.",
        );
      return {
        url: file.url,
        mediaType: file.mediaType,
        bytes,
        blobId: file.blobId,
      };
    }),
  );
}

/**
 * Sanitize every persisted evidence field, including URLs inside prompt text.
 *
 * A string that is a declared file URL becomes its captured reference; every
 * other URL keeps its text with its credentials redacted.
 */
export function createRecordedEvidenceSanitizer(
  references: ReadonlyMap<string, string>,
  onUnsupportedEvidence: () => void,
): RecordedEvidenceSanitizer {
  const lookup = createFileUrlLookup(references);
  function replaceString(value: string): string {
    return lookup(value) ?? redactUrlCredentials(value);
  }
  function replace<T>(value: T): T {
    const active = new Set<object>();
    function visit(current: unknown): unknown {
      if (typeof current === "string") return replaceString(current);
      if (current instanceof URL) return replaceString(current.href);
      // A content reference holds no URL or credential.
      if (
        current instanceof Date ||
        current instanceof Uint8Array ||
        current instanceof InlineFileContent
      )
        return current;
      if (
        current instanceof ArrayBuffer ||
        typeof current === "function" ||
        typeof current === "symbol"
      ) {
        onUnsupportedEvidence();
        return "[unrecordable evidence value]";
      }
      if (current === null || typeof current !== "object") return current;
      if (active.has(current)) {
        onUnsupportedEvidence();
        return "[unrecordable circular evidence]";
      }
      active.add(current);
      try {
        if (Array.isArray(current)) {
          if (
            Object.values(Object.getOwnPropertyDescriptors(current)).some(
              (descriptor) => descriptor.enumerable && !("value" in descriptor),
            )
          ) {
            onUnsupportedEvidence();
            return "[unrecordable accessor evidence]";
          }
          return current.map(visit);
        }
        if (
          (Object.getPrototypeOf(current) !== Object.prototype &&
            Object.getPrototypeOf(current) !== null) ||
          Reflect.ownKeys(current).some((key) => typeof key !== "string")
        ) {
          onUnsupportedEvidence();
          return "[unrecordable evidence value]";
        }
        const descriptors = Object.getOwnPropertyDescriptors(current);
        if (
          Object.values(descriptors).some(
            (descriptor) => descriptor.enumerable && !("value" in descriptor),
          )
        ) {
          onUnsupportedEvidence();
          return "[unrecordable accessor evidence]";
        }
        return Object.fromEntries(
          Object.entries(current).map(([key, item]) => [
            replaceString(key),
            visit(item),
          ]),
        );
      } finally {
        active.delete(current);
      }
    }
    return visit(value) as T;
  }
  return { replace };
}

const NETWORK_URL = /^https?:\/\//i;

function isFilePart(value: Record<string, unknown>): boolean {
  return value.type === "file" || value.type === "image";
}

/** Whether a stored message's content holds a file part of its own. */
function hasFilePart(content: Record<string, unknown>): boolean {
  return (
    Array.isArray(content.parts) &&
    content.parts.some(
      (part) =>
        typeof part === "object" &&
        part !== null &&
        (part as Record<string, unknown>).type === "file",
    )
  );
}

/**
 * The URLs matching `pattern` held by file or image parts in `value`, or by
 * message attachments, in the order they appear.
 *
 * With `sentAttachmentsOnly`, attachments of a message that also holds a
 * file part are skipped: Mastra builds model parts from a stored message's
 * `experimental_attachments` only when its parts hold no file.
 */
function collectFileUrls(
  value: unknown,
  pattern: RegExp,
  sentAttachmentsOnly = false,
): string[] {
  const urls: string[] = [];
  const active = new Set<object>();
  function visit(current: unknown, filePart: boolean): void {
    if (typeof current === "string" || current instanceof URL) {
      const url = typeof current === "string" ? current : current.href;
      if (filePart && pattern.test(url)) urls.push(url);
      return;
    }
    if (
      current === null ||
      typeof current !== "object" ||
      current instanceof Date ||
      current instanceof Uint8Array ||
      active.has(current)
    )
      return;
    active.add(current);
    try {
      if (Array.isArray(current)) {
        for (const item of current) visit(item, filePart);
        return;
      }
      const record = current as Record<string, unknown>;
      const part = filePart || isFilePart(record);
      const attachmentsSent = !sentAttachmentsOnly || !hasFilePart(record);
      for (const [key, item] of Object.entries(record))
        visit(
          item,
          part || (key === "experimental_attachments" && attachmentsSent),
        );
    } finally {
      active.delete(current);
    }
  }
  visit(value, false);
  return urls;
}

/**
 * The network URLs held by file or image parts in `value`, or by message
 * attachments. Only captured files become recorded references, so replay
 * would have to fetch any other such URL.
 */
export function collectFileNetworkUrls(value: unknown): string[] {
  return collectFileUrls(value, NETWORK_URL);
}

const MODEL_FILE_URL = /^(?:https?|kitaru-file):\/\//i;

/**
 * Whether a file or image part in `value`, or a message attachment that
 * Mastra sends to the model, holds a network URL or a captured file reference
 * instead of the file's content.
 *
 * Mastra hands such a URL to the model provider, or downloads it itself,
 * without the application's `resolveFile`. A replay's messages hold the
 * captured reference there, which neither the provider nor Mastra can fetch.
 */
export function containsModelFileUrl(value: unknown): boolean {
  return collectFileUrls(value, MODEL_FILE_URL, true).length > 0;
}

/**
 * Copy invocation input with each captured file reference held as the `data`
 * or `image` of a file or image part turned into a `URL`.
 *
 * Mastra reads such a string as base64 content unless it starts with `http`,
 * `data:` or `file-`, so a reference string would reach processors as a
 * `data:` URL. As a `URL`, Mastra keeps it as the reference string, the way
 * it kept the network URL the baseline sent.
 */
export function referenceInputFilesAsUrls<T>(value: T): T {
  function visit(current: unknown): unknown {
    if (Array.isArray(current)) return current.map(visit);
    if (
      current === null ||
      typeof current !== "object" ||
      Object.getPrototypeOf(current) !== Object.prototype
    )
      return current;
    const entries = Object.entries(current).map(([key, item]) => [
      key,
      isFilePart(current as Record<string, unknown>) &&
      (key === "data" || key === "image") &&
      typeof item === "string" &&
      FILE_REFERENCE.test(item)
        ? new URL(item)
        : visit(item),
    ]);
    return Object.fromEntries(entries);
  }
  return visit(value) as T;
}

/** Declared files did not finish downloading within the capture wait. */
export class FileCaptureTimeoutError extends Error {
  constructor() {
    super("Controlled file capture timed out.");
  }
}

/**
 * One turn's downloads of declared files, shared between capture and a native
 * fallback so a file is not fetched from its URL twice.
 */
export function createFileDownloads(resolveFile: MemoryFileResolver) {
  const started = new Map<string, Promise<ResolvedMemoryFile>>();
  return {
    /** Start the download of `url`, or join the one already running. */
    capture(url: string): Promise<ResolvedMemoryFile> {
      const key = normalizeFileUrl(url);
      let download = started.get(key);
      if (!download) {
        download = Promise.resolve().then(() => resolveFile(url));
        // Capture can stop waiting for a download it started.
        download.catch(() => undefined);
        started.set(key, download);
      }
      return download;
    },
    /**
     * Resolve `url` for a native turn. The first request for a URL capture
     * started takes over that download, still running or finished; a failed
     * download is fetched again, as the native turn would.
     */
    resolveNative(url: string): Promise<ResolvedMemoryFile> {
      const key = normalizeFileUrl(url);
      const download = started.get(key);
      if (!download) return resolveFile(url);
      started.delete(key);
      return download.catch(() => resolveFile(url));
    },
  };
}

/**
 * Capture declared URLs once and convert persisted input to secret-free references.
 *
 * Downloads run concurrently. With `waitMs`, capture rejects with
 * `FileCaptureTimeoutError` once that long passes before every download has
 * finished, and leaves the unfinished downloads running.
 */
export async function createCapturedFiles(
  urls: readonly string[],
  download: MemoryFileResolver,
  waitMs?: number,
) {
  // Keyed by WHATWG form so a file part holding `new URL(declared)` matches.
  const declaredToReference = new Map<string, string>();
  const filesByReference = new Map<string, MastraRecordedFile>();
  const uniqueUrls = new Map<string, string>();
  for (const url of urls) {
    const normalized = normalizeFileUrl(url);
    if (!uniqueUrls.has(normalized)) uniqueUrls.set(normalized, url);
  }
  if (uniqueUrls.size > MAX_RECORDED_FILES)
    throw new Error(
      "Unsupported Mastra memory replay: file count limit exceeded.",
    );
  const downloads = [...uniqueUrls].map(([normalized, url]) => {
    const pending = Promise.resolve().then(() => download(url));
    pending.catch(() => undefined);
    return [normalized, pending] as const;
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline =
    waitMs === undefined || downloads.length === 0
      ? undefined
      : new Promise<never>((_resolve, reject) => {
          timer = setTimeout(
            () => reject(new FileCaptureTimeoutError()),
            waitMs,
          );
        });
  deadline?.catch(() => undefined);
  const conversationUrls = new Set<string>();
  let totalBytes = 0;
  /**
   * Check a downloaded file against the capture limits and return its record.
   * `totalBytes` is only updated by the caller once the whole batch fits.
   */
  function checkedFile(
    resolved: ResolvedMemoryFile,
    bytesBefore: number,
  ): MastraRecordedFile {
    if (
      !(resolved.bytes instanceof Uint8Array) ||
      typeof resolved.mediaType !== "string" ||
      !resolved.mediaType
    )
      throw new TypeError("File resolver must return bytes and mediaType");
    if (bytesBefore + resolved.bytes.byteLength > MAX_CAPTURED_FILE_BYTES)
      throw new Error("Unsupported Mastra memory replay: files exceed 16 MiB.");
    return {
      url: fileReference(resolved),
      bytes: new Uint8Array(resolved.bytes),
      mediaType: resolved.mediaType,
    };
  }
  try {
    for (const [normalized, pending] of downloads) {
      let resolved: ResolvedMemoryFile;
      try {
        resolved = await (deadline
          ? Promise.race([pending, deadline])
          : pending);
      } catch (error) {
        if (error instanceof FileCaptureTimeoutError) throw error;
        // Resolver errors can contain a signed URL. Keep them out of diagnostics.
        throw new Error("Controlled file capture failed.");
      }
      const file = checkedFile(resolved, totalBytes);
      totalBytes += file.bytes.byteLength;
      declaredToReference.set(normalized, file.url);
      filesByReference.set(file.url, file);
    }
  } finally {
    clearTimeout(timer);
  }
  const lookup = createFileUrlLookup(declaredToReference);
  function referenceFor(url: string): string {
    const reference = lookup(url);
    if (!reference)
      throw new Error("Unsupported Mastra memory replay: undeclared file URL.");
    return reference;
  }
  /**
   * Copy replay input with declared file URLs swapped for their references.
   *
   * Only a whole value that is a declared URL is swapped, so prompt text,
   * working memory and neighboring URLs keep what the model saw; their URL
   * credentials are redacted instead. An accepted conversation URL not
   * captured yet keeps its redacted form. Any other network URL inside a
   * file or image part throws, because replay would have to fetch it.
   */
  function replaceDeclaredFileUrls<T>(value: T): T {
    const active = new Set<object>();
    function visit(current: unknown, filePart: boolean): unknown {
      if (typeof current === "string") {
        const reference = lookup(current);
        if (reference) return reference;
        if (
          filePart &&
          NETWORK_URL.test(current) &&
          !conversationUrls.has(normalizeFileUrl(current))
        )
          throw new MastraReplayReasonError(
            "Unsupported Mastra memory replay: undeclared file URL.",
            "file_url_undeclared",
          );
        return redactUrlCredentials(current);
      }
      if (current instanceof URL) {
        const reference = lookup(current.href);
        if (reference) return new URL(reference);
        if (
          filePart &&
          /^https?:$/i.test(current.protocol) &&
          !conversationUrls.has(normalizeFileUrl(current.href))
        )
          throw new MastraReplayReasonError(
            "Unsupported Mastra memory replay: undeclared file URL.",
            "file_url_undeclared",
          );
        const redacted = redactUrlCredentials(current.href);
        return redacted === current.href ? current : new URL(redacted);
      }
      if (
        current === null ||
        typeof current !== "object" ||
        current instanceof Date ||
        current instanceof Uint8Array
      )
        return current;
      if (active.has(current))
        throw new Error(
          "Unsupported Mastra memory replay: circular file input.",
        );
      active.add(current);
      try {
        if (Array.isArray(current)) {
          if (
            Reflect.ownKeys(current).some((key) => {
              const descriptor = Object.getOwnPropertyDescriptor(current, key);
              return descriptor && !("value" in descriptor);
            })
          )
            throw new Error(
              "Unsupported Mastra memory replay: file input accessor.",
            );
          return current.map((item) => visit(item, filePart));
        }
        if (
          (Object.getPrototypeOf(current) !== Object.prototype &&
            Object.getPrototypeOf(current) !== null) ||
          Reflect.ownKeys(current).some((key) => typeof key !== "string")
        )
          throw new Error("Unsupported Mastra memory replay: file input type.");
        const descriptors = Object.getOwnPropertyDescriptors(current);
        const part =
          filePart ||
          descriptors.type?.value === "file" ||
          descriptors.type?.value === "image";
        const entries = Object.entries(descriptors);
        return Object.fromEntries(
          entries.map(([key, descriptor]) => {
            if (!descriptor.enumerable || !("value" in descriptor))
              throw new Error(
                "Unsupported Mastra memory replay: file input accessor.",
              );
            return [key, visit(descriptor.value, part)];
          }),
        );
      } finally {
        active.delete(current);
      }
    }
    return visit(value, false) as T;
  }
  return {
    get files(): MastraRecordedFile[] {
      return restoreCapturedFiles([...filesByReference.values()]).files;
    },
    /** Whether the turn captured a file with this content reference. */
    hasFile: (reference: string): boolean => filesByReference.has(reference),
    /**
     * Allow the turn to capture the network URLs held by file parts in its
     * input or thread history once it resolves them.
     *
     * These URLs are part of the recorded invocation, so capturing them adds
     * no way for replay to fetch an arbitrary URL.
     */
    acceptConversationUrls(urls: readonly string[]): void {
      for (const url of urls) {
        const normalized = normalizeFileUrl(url);
        if (!declaredToReference.has(normalized))
          conversationUrls.add(normalized);
      }
    },
    /** Whether `url` is an accepted conversation URL not captured yet. */
    isConversationUrl: (url: string): boolean =>
      conversationUrls.has(normalizeFileUrl(url)) && lookup(url) === undefined,
    /**
     * Declare the conversation URL `url` with the file the turn resolved for
     * it. Throws with reason `file_capture_failed`, and declares nothing, when
     * the file breaks the capture limits.
     */
    recordConversationFile(url: string, resolved: ResolvedMemoryFile): void {
      const normalized = normalizeFileUrl(url);
      if (declaredToReference.has(normalized)) return;
      let file: MastraRecordedFile;
      try {
        if (declaredToReference.size >= MAX_RECORDED_FILES)
          throw new Error(
            "Unsupported Mastra memory replay: file count limit exceeded.",
          );
        file = checkedFile(resolved, totalBytes);
      } catch (error) {
        throw new MastraReplayReasonError(
          `A conversation file could not be captured: ${
            error instanceof Error ? error.message : "invalid file."
          }`,
          "file_capture_failed",
        );
      }
      totalBytes += file.bytes.byteLength;
      declaredToReference.set(normalized, file.url);
      filesByReference.set(file.url, file);
    },
    /** Whether `url` is one of the declared file URLs. */
    isDeclared: (url: string): boolean => lookup(url) !== undefined,
    referenceFor,
    evidenceSanitizer: (onUnsupportedEvidence: () => void) =>
      createRecordedEvidenceSanitizer(
        declaredToReference,
        onUnsupportedEvidence,
      ),
    replaceDeclaredFileUrls,
    resolveFile: async (url: string): Promise<ResolvedMemoryFile> => {
      const file = filesByReference.get(referenceFor(url));
      if (!file)
        throw new Error(
          "Unsupported Mastra memory replay: file reference was not recorded.",
        );
      return { bytes: new Uint8Array(file.bytes), mediaType: file.mediaType };
    },
  };
}
