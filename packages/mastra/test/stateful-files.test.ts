import { createHash } from "node:crypto";
import { expect, it, vi } from "vitest";
import {
  decodeMemoryValue,
  encodeMemoryValue,
} from "../src/memory-snapshot.js";
import {
  createCapturedFiles,
  createFileBlobStore,
  createInlineFileReader,
  type FileBlobClient,
  fileReference,
  loadRecordedFiles,
  referenceInlineFiles,
  restoreCapturedFiles,
  restoreInlineFiles,
  type StoredBlob,
} from "../src/stateful-files.js";

it("captures signed URLs under secret-free references and replays immutable bytes", async () => {
  const resolver = vi.fn(async () => ({
    bytes: new Uint8Array([0, 255, 1]),
    mediaType: "application/pdf",
  }));
  const signedUrl = "https://files.invalid/a?token=PRIVATE_TOKEN";
  const captured = await createCapturedFiles([signedUrl, signedUrl], resolver);
  const reference = captured.referenceFor(signedUrl);
  expect(reference).toMatch(/^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/);
  expect(JSON.stringify(captured.files)).not.toContain("PRIVATE_TOKEN");
  expect(JSON.stringify(captured.files)).not.toContain("files.invalid");
  const replay = restoreCapturedFiles(captured.files);
  (await captured.resolveFile(signedUrl)).bytes[0] = 9;
  expect((await replay.resolveFile(reference)).bytes).toEqual(
    new Uint8Array([0, 255, 1]),
  );
  await expect(replay.resolveFile(signedUrl)).rejects.toThrow(/not recorded/);
  expect(resolver).toHaveBeenCalledTimes(1);
});

it("rewrites declared file and image URLs without changing baseline input", async () => {
  const signedUrl = "https://files.invalid/a?X-Amz-Signature=SECRET";
  const captured = await createCapturedFiles([signedUrl], async () => ({
    bytes: new Uint8Array([1, 2]),
    mediaType: "image/png",
  }));
  const baseline = {
    messages: [
      { role: "user", content: [{ type: "image", image: new URL(signedUrl) }] },
    ],
    context: { attachment: signedUrl },
  };
  const historical = captured.replaceDeclaredFileUrls(baseline);
  expect(baseline.messages[0]?.content[0]?.image.href).toBe(signedUrl);
  expect(historical.messages[0]?.content[0]?.image.href).toBe(
    captured.referenceFor(signedUrl),
  );
  expect(historical.context.attachment).toBe(captured.referenceFor(signedUrl));
  expect(JSON.stringify(historical)).not.toContain("SECRET");
  await expect(
    Promise.resolve().then(() =>
      captured.replaceDeclaredFileUrls({
        type: "file",
        data: "https://files.invalid/undeclared?token=SECRET",
      }),
    ),
  ).rejects.toThrow(/undeclared file URL/);
});

it("maps whole declared values to references and redacts other evidence URLs without flagging", async () => {
  const signed = "https://files.invalid/a?token=KNOWN_SECRET";
  const captured = await createCapturedFiles([signed], async () => ({
    bytes: new Uint8Array([1]),
    mediaType: "image/png",
  }));
  const unsupported = vi.fn();
  const sanitizer = captured.evidenceSanitizer(unsupported);
  const native = {
    prompt: `Open ${signed} then https://other.invalid/b?token=UNKNOWN_SECRET`,
    tool: { result: signed },
  };
  const persisted = sanitizer.replace(native);
  expect(native.prompt).toContain("KNOWN_SECRET");
  expect(persisted.prompt).toBe(
    "Open https://files.invalid/a?token=REDACTED then https://other.invalid/b?token=REDACTED",
  );
  expect(persisted.tool.result).toBe(captured.referenceFor(signed));
  expect(JSON.stringify(persisted)).not.toMatch(/KNOWN_SECRET|UNKNOWN_SECRET/);
  expect(unsupported).not.toHaveBeenCalled();
});

it("leaves prompt text, working memory and neighboring URLs as the model saw them", async () => {
  const declared = "https://cdn.example.com/q3";
  const captured = await createCapturedFiles([declared], async () => ({
    bytes: new Uint8Array([1]),
    mediaType: "application/pdf",
  }));
  const text = `Summarize ${declared} please; draft at ${declared}-draft.pdf and ${declared}?X-Amz-Signature=PREFIX_SECRET`;
  const input = {
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text },
          { type: "file", data: declared, mimeType: "application/pdf" },
        ],
      },
    ],
    workingMemory: `Q3 report lives at ${declared}`,
  };
  const replayed = captured.replaceDeclaredFileUrls(input);
  const reference = captured.referenceFor(declared);
  expect(replayed.messages[0]?.content[0]).toEqual({
    type: "text",
    text: `Summarize ${declared} please; draft at ${declared}-draft.pdf and ${declared}?X-Amz-Signature=REDACTED`,
  });
  expect(replayed.messages[0]?.content[1]).toMatchObject({ data: reference });
  expect(replayed.workingMemory).toBe(input.workingMemory);
  const evidence = captured.evidenceSanitizer(vi.fn()).replace(input);
  expect(evidence.messages[0]?.content[0]).toEqual(
    replayed.messages[0]?.content[0],
  );
  expect(evidence.workingMemory).toBe(input.workingMemory);
  expect(JSON.stringify([replayed, evidence])).not.toContain("PREFIX_SECRET");
});

it.each([
  "https://files.example.com/Q3 Report.pdf",
  "https://Files.Example.com/a.pdf",
  "https://files.example.com/résumé.pdf?token=NORMALIZED_SECRET",
])("matches a declared URL in its WHATWG form: %s", async (declared) => {
  const resolveFile = vi.fn(async () => ({
    bytes: new Uint8Array([7]),
    mediaType: "application/pdf",
  }));
  const captured = await createCapturedFiles(
    [declared, new URL(declared).href],
    resolveFile,
  );
  expect(resolveFile).toHaveBeenCalledOnce();
  expect(resolveFile).toHaveBeenCalledWith(declared);
  const reference = captured.referenceFor(declared);
  const replayed = captured.replaceDeclaredFileUrls({
    type: "file",
    data: new URL(declared),
  });
  expect(replayed.data.href).toBe(reference);
  expect(captured.referenceFor(new URL(declared).href)).toBe(reference);
  expect((await captured.resolveFile(new URL(declared).href)).bytes).toEqual(
    new Uint8Array([7]),
  );
  expect(
    captured.evidenceSanitizer(vi.fn()).replace({ url: new URL(declared) }),
  ).toEqual({ url: reference });
});

it("preserves Date and bytes through signed URL evidence projection", async () => {
  const signed = "https://files.invalid/photo?token=SECRET";
  const captured = await createCapturedFiles([signed], async () => ({
    bytes: new Uint8Array([0, 255]),
    mediaType: "image/png",
  }));
  const unknown = vi.fn();
  const date = new Date("2025-01-01T00:00:00.000Z");
  const bytes = new Uint8Array([1, 0, 255]);
  const value = captured.evidenceSanitizer(unknown).replace({
    date,
    bytes,
    url: signed,
  });
  expect(decodeMemoryValue(encodeMemoryValue(value))).toEqual({
    date,
    bytes,
    url: captured.referenceFor(signed),
  });
  expect(unknown).not.toHaveBeenCalled();
  const unsupported = captured.evidenceSanitizer(unknown).replace({
    value: new Map([["key", signed]]),
  });
  expect(unsupported.value).toBe("[unrecordable evidence value]");
  expect(unknown).toHaveBeenCalledOnce();
});

it("rejects altered file bytes and hides resolver errors containing URL secrets", async () => {
  const url = "https://files.invalid/a?token=SECRET";
  const captured = await createCapturedFiles([url], async () => ({
    bytes: new Uint8Array([1]),
    mediaType: "image/png",
  }));
  const files = captured.files;
  const file = files[0];
  if (!file) throw new Error("Missing captured file");
  file.bytes[0] = 2;
  expect(() => restoreCapturedFiles(files)).toThrow(/invalid recorded file/);
  await expect(
    createCapturedFiles([url], async () => {
      throw new Error(`Failed to fetch ${url}`);
    }),
  ).rejects.toThrow(/^Controlled file capture failed\.$/);
});

it("rejects cyclic or accessor-backed replay input without reading the accessor", async () => {
  const captured = await createCapturedFiles([], async () => {
    throw new Error("unexpected resolver call");
  });
  const cycle: unknown[] = [];
  cycle.push(cycle);
  expect(() => captured.replaceDeclaredFileUrls(cycle)).toThrow(/circular/);
  const getter = vi.fn(() => "https://files.invalid/a?token=SECRET");
  const part = Object.defineProperty({ type: "file" }, "data", {
    enumerable: true,
    get: getter,
  });
  expect(() => captured.replaceDeclaredFileUrls(part)).toThrow(/accessor/);
  expect(getter).not.toHaveBeenCalled();
});

it("accepts one file up to the 16 MiB turn limit and rejects a larger one", async () => {
  const fits = new Uint8Array(16 * 1024 * 1024);
  const captured = await createCapturedFiles(
    ["https://files.invalid/fits"],
    async () => ({ bytes: fits, mediaType: "application/pdf" }),
  );
  expect(captured.files[0]?.bytes.byteLength).toBe(fits.byteLength);
  const bytes = new Uint8Array(16 * 1024 * 1024 + 1);
  await expect(
    createCapturedFiles(["https://files.invalid/large"], async () => ({
      bytes,
      mediaType: "application/pdf",
    })),
  ).rejects.toThrow(/16 MiB/);
  expect(() =>
    restoreCapturedFiles([
      {
        url: fileReference({ bytes, mediaType: "application/pdf" }),
        bytes,
        mediaType: "application/pdf",
      },
    ]),
  ).toThrow(/file capture limit exceeded/);
});

it("rejects more than 16 MiB or 64 declared files before retaining them", async () => {
  const bytes = new Uint8Array(6 * 1024 * 1024);
  const resolveFile = vi.fn(async () => ({ bytes, mediaType: "image/png" }));
  await expect(
    createCapturedFiles(
      [
        "https://files.invalid/1",
        "https://files.invalid/2",
        "https://files.invalid/3",
      ],
      resolveFile,
    ),
  ).rejects.toThrow(/16 MiB/);
  expect(resolveFile).toHaveBeenCalledTimes(3);
  resolveFile.mockClear();
  await expect(
    createCapturedFiles(
      Array.from(
        { length: 65 },
        (_, index) => `https://files.invalid/${index}`,
      ),
      resolveFile,
    ),
  ).rejects.toThrow(/count limit/);
  expect(resolveFile).not.toHaveBeenCalled();
});

/** An in-memory blob API that keeps one blob per content, as the server does. */
function createBlobApi() {
  const blobs = new Map<string, StoredBlob & { bytes: Uint8Array }>();
  const client = {
    upload: vi.fn(async (content: Uint8Array) => {
      const sha256 = createHash("sha256").update(content).digest("hex");
      const existing = [...blobs.values()].find(
        (blob) => blob.sha256 === sha256,
      );
      if (existing) return existing;
      const blob = {
        id: `018f0000-0000-7000-8002-${String(blobs.size).padStart(12, "0")}`,
        sha256,
        size: content.byteLength,
        bytes: new Uint8Array(content),
      };
      blobs.set(blob.id, blob);
      return blob;
    }),
    get: vi.fn(async (blobId: string) => {
      const blob = blobs.get(blobId);
      if (!blob) throw new Error("Blob not found");
      return blob;
    }),
    download: vi.fn(async (blobId: string) => {
      const blob = blobs.get(blobId);
      if (!blob) throw new Error("Blob not found");
      return new Uint8Array(blob.bytes);
    }),
  } satisfies FileBlobClient;
  return { blobs, client };
}

function recordedFile(bytes: Uint8Array) {
  return {
    url: fileReference({ bytes, mediaType: "image/png" }),
    mediaType: "image/png",
    bytes,
  };
}

it("uploads a captured file once across turns and again after its blob is deleted", async () => {
  const { blobs, client } = createBlobApi();
  const store = createFileBlobStore(client);
  const file = recordedFile(new Uint8Array([1, 2, 3]));
  const [first] = await store.store([file]);
  const [second] = await store.store([recordedFile(new Uint8Array([1, 2, 3]))]);
  expect(second?.blobId).toBe(first?.blobId);
  expect(client.upload).toHaveBeenCalledOnce();

  blobs.clear();
  const [third] = await store.store([file]);
  expect(client.upload).toHaveBeenCalledTimes(2);
  expect(blobs.has(String(third?.blobId))).toBe(true);
});

it("refuses a stored blob that does not hold the captured bytes", async () => {
  const { client } = createBlobApi();
  client.upload.mockResolvedValueOnce({
    id: "018f0000-0000-7000-8002-000000000999",
    sha256: "0".repeat(64),
    size: 3,
    bytes: new Uint8Array(3),
  });
  await expect(
    createFileBlobStore(client).store([
      recordedFile(new Uint8Array([1, 2, 3])),
    ]),
  ).rejects.toMatchObject({ reason: "file_store_failed" });
});

it("loads stored files and refuses blobs whose content changed", async () => {
  const { blobs, client } = createBlobApi();
  const bytes = new Uint8Array([4, 5, 6]);
  const [stored] = await createFileBlobStore(client).store([
    recordedFile(bytes),
  ]);
  if (!stored?.blobId) throw new Error("Missing stored file");
  const source = {
    url: stored.url,
    mediaType: stored.mediaType,
    blobId: stored.blobId,
    length: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  };
  expect(await loadRecordedFiles([source], client)).toEqual([
    { ...recordedFile(bytes), blobId: stored.blobId },
  ]);
  const blob = blobs.get(stored.blobId);
  if (!blob) throw new Error("Missing blob");
  blob.bytes = new Uint8Array([4, 5, 7]);
  await expect(loadRecordedFiles([source], client)).rejects.toThrow(
    /does not match its reference/,
  );
  blobs.clear();
  await expect(loadRecordedFiles([source], client)).rejects.toThrow(
    /could not be downloaded/,
  );
  await expect(
    loadRecordedFiles([{ ...source, length: 16 * 1024 * 1024 + 1 }], client),
  ).rejects.toThrow(/capture limit/);
});

it("records known inline history files by reference and writes them back exactly", () => {
  const bytes = new Uint8Array([37, 80, 68, 70, 0, 255, 7]);
  const reference = fileReference({ bytes, mediaType: "application/pdf" });
  const base64 = Buffer.from(bytes).toString("base64");
  const unknown = Buffer.from([1, 2, 3]).toString("base64");
  const parts = [
    { type: "file", data: base64, mimeType: "application/pdf" },
    {
      type: "file",
      data: `data:application/pdf;base64,${base64}`,
      mediaType: "application/pdf",
    },
    { type: "file", data: new Uint8Array(bytes), mediaType: "application/pdf" },
    // Base64 that re-encodes differently stays as the application wrote it.
    { type: "file", data: `${base64}\n`, mimeType: "application/pdf" },
    { type: "file", data: unknown, mimeType: "application/pdf" },
  ];
  const snapshot = {
    messages: [{ id: "m", content: { format: 2, parts } }],
  };
  const referenced = referenceInlineFiles(snapshot, {
    read: createInlineFileReader(),
    isKnown: (value) => value === reference,
    files: [],
  });
  // A file from an earlier turn joins the recorded files once.
  expect(referenced.files.map((file) => file.url)).toEqual([reference]);
  const encoded = JSON.stringify(encodeMemoryValue(referenced.snapshot));
  // Only the non-canonical copy still holds the bytes.
  expect(encoded.split(base64)).toHaveLength(2);
  expect(encoded).toContain(JSON.stringify(`${base64}\n`));
  expect(encoded).toContain(unknown);
  const decoded = decodeMemoryValue(JSON.parse(encoded));
  const files = restoreCapturedFiles(referenced.files);
  expect(restoreInlineFiles(decoded, files.readFile)).toEqual(snapshot);
  expect(() => restoreInlineFiles(decoded, () => undefined)).toThrow(
    /inline file content was not recorded/,
  );
});
