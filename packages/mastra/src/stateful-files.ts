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
const MAX_RECORDED_FILES = 64;
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
  const data = part.type === "image" ? part.image : part.data;
  const cached = cache.get(data);
  if (cached) return cached;
  let bytes: Uint8Array;
  if (data instanceof Uint8Array) bytes = data;
  else if (typeof data === "string" && data.startsWith("data:"))
    bytes = Buffer.from(data.slice(data.indexOf(",") + 1), "base64");
  else if (typeof data === "string" && !/^[a-z][a-z0-9+.-]*:/i.test(data))
    bytes = Buffer.from(data, "base64");
  else return undefined;
  const mediaType = [part.mediaType, part.mimeType].find(
    (value) => typeof value === "string",
  );
  const reference = fileReference({
    bytes,
    mediaType: typeof mediaType === "string" ? mediaType : "",
  });
  cache.set(data, reference);
  return reference;
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
): <T>(value: T) => T {
  const cache = new Map<unknown, string>();
  function visit(current: unknown): unknown {
    if (Array.isArray(current)) {
      const items = current.map(visit);
      return items.some((item, index) => item !== current[index])
        ? items
        : current;
    }
    if (
      current === null ||
      typeof current !== "object" ||
      Object.getPrototypeOf(current) !== Object.prototype
    )
      return current;
    const record = current as Record<string, unknown>;
    if (isFilePart(record)) {
      const reference = getInlineFileReference(record, cache);
      if (!reference || !isCaptured(reference)) return current;
      return {
        ...record,
        [record.type === "image" ? "image" : "data"]: reference,
      };
    }
    let changed = false;
    const entries = Object.entries(record).map(([key, item]) => {
      const next = visit(item);
      if (next !== item) changed = true;
      return [key, next] as const;
    });
    return changed ? Object.fromEntries(entries) : current;
  }
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
      if (current instanceof Date || current instanceof Uint8Array)
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

/**
 * The URLs matching `pattern` held by file or image parts in `value`, or by
 * message attachments, in the order they appear.
 */
function collectFileUrls(value: unknown, pattern: RegExp): string[] {
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
      const part = filePart || isFilePart(current as Record<string, unknown>);
      for (const [key, item] of Object.entries(current))
        visit(item, part || key === "experimental_attachments");
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
 * Whether a file or image part in `value`, or a message attachment, holds a
 * network URL or a captured file reference instead of the file's content.
 *
 * Mastra hands such a URL to the model provider, or downloads it itself,
 * without the application's `resolveFile`. A replay's messages hold the
 * captured reference there, which neither the provider nor Mastra can fetch.
 */
export function containsModelFileUrl(value: unknown): boolean {
  return collectFileUrls(value, MODEL_FILE_URL).length > 0;
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
  const historyUrls = new Set<string>();
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
   * credentials are redacted instead. An undeclared network URL inside a
   * file or image part throws, because replay would have to fetch it.
   */
  function replaceDeclaredFileUrls<T>(value: T): T {
    const active = new Set<object>();
    function visit(current: unknown, filePart: boolean): unknown {
      if (typeof current === "string") {
        const reference = lookup(current);
        if (reference) return reference;
        if (filePart && NETWORK_URL.test(current))
          throw new MastraReplayReasonError(
            "Unsupported Mastra memory replay: undeclared file URL.",
            "file_url_undeclared",
          );
        return redactUrlCredentials(current);
      }
      if (current instanceof URL) {
        const reference = lookup(current.href);
        if (reference) return new URL(reference);
        if (filePart && /^https?:$/i.test(current.protocol))
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
     * thread history once it resolves them.
     *
     * History URLs are part of the recorded conversation, so capturing them
     * adds no way for replay to fetch an arbitrary URL.
     */
    acceptHistoryUrls(urls: readonly string[]): void {
      for (const url of urls) {
        const normalized = normalizeFileUrl(url);
        if (!declaredToReference.has(normalized)) historyUrls.add(normalized);
      }
    },
    /** Whether `url` is an accepted history URL not captured yet. */
    isHistoryUrl: (url: string): boolean =>
      historyUrls.has(normalizeFileUrl(url)) && lookup(url) === undefined,
    /**
     * Declare the history URL `url` with the file the turn resolved for it.
     * Throws with reason `file_capture_failed`, and declares nothing, when the
     * file breaks the capture limits.
     */
    recordHistoryFile(url: string, resolved: ResolvedMemoryFile): void {
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
          `A thread history file could not be captured: ${
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
