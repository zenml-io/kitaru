import { createHash } from "node:crypto";
import { redactUrlCredentials } from "@zenml-io/kitaru/adapter";
import {
  createInlineFileReader,
  fileReference,
  type InlineFileReader,
} from "./stateful-files.js";

const VOLATILE_KEYS = new Set(["createdAt", "updatedAt", "abortSignal"]);
const VOLATILE_TEXT: ReadonlyArray<readonly [RegExp, string]> = [
  [
    /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?/g,
    "<timestamp>",
  ],
  [
    /\b(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,? )?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d{1,2},? \d{4}\b/g,
    "<date>",
  ],
  [/\b\d{1,2}:\d{2}(?::\d{2})?\s?[AP]M\b/gi, "<time>"],
  [
    /\b\d+ (?:second|minute|hour|day|week|month|year)s? (?:ago|later)\b/g,
    "<relative-time>",
  ],
  [
    /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi,
    "<id>",
  ],
];

// Mastra labels an attachment by its file name or URL, and a replay's history
// holds the captured reference in place of the URL. The part's data still
// identifies the file.
const ATTACHMENT_LABEL = /\[(File|Image) #(\d+): [^\]\n]*\]/g;

// Mastra writes a date header line and a time label only where the day or
// minute differs from the line before, so whether one appears depends on where
// day and minute boundaries fell during the run. These match the placeholders
// that VOLATILE_TEXT leaves.
const CLOCK_MARKERS: readonly RegExp[] = [
  /^<date>:\n/gm,
  /^\(<time>\): /gm,
  / \(<time>\)(?=: )/g,
];

/**
 * How a fingerprint treats Mastra's date headers and time labels.
 *
 * `legacy` keeps them, as tapes recorded before they were dropped hashed them.
 */
export type OMFingerprintFormat = "current" | "legacy";

function normalizeText(text: string, format: OMFingerprintFormat): string {
  // Replay history holds redacted URLs, so the baseline hashes them redacted too.
  const normalized = VOLATILE_TEXT.reduce(
    (current, [pattern, replacement]) => current.replace(pattern, replacement),
    redactUrlCredentials(text),
  ).replace(ATTACHMENT_LABEL, "[$1 #$2]");
  return format === "legacy"
    ? normalized
    : CLOCK_MARKERS.reduce(
        (current, pattern) => current.replace(pattern, ""),
        normalized,
      );
}

/**
 * Hash an OM call's input without the values that change on every run.
 *
 * Mastra renders message times and ids into the observer prompt and stamps
 * each prompt part with its creation time, so an unchanged replay would never
 * match its baseline if those values were hashed. Attachment bytes hash as
 * their content reference, the form a replay's recorded history holds.
 */
export function getOMInputFingerprint(
  input: unknown,
  mapString: (value: string) => string = (value) => value,
  onFileContent?: (reference: string) => void,
  readInlineFile: InlineFileReader = createInlineFileReader(),
  format: OMFingerprintFormat = "current",
): string {
  try {
    const text = JSON.stringify(
      input,
      function (this: unknown, key: string, value: unknown) {
        if (VOLATILE_KEYS.has(key)) return undefined;
        // The holder keeps the bytes as they were before a Buffer's toJSON.
        const holder =
          typeof this === "object" && this !== null
            ? (this as Record<string, unknown>)
            : undefined;
        // Inline attachment text hashes as its content reference, so the
        // same file hashes alike as base64, a data URL, or bytes.
        if (
          typeof value === "string" &&
          holder &&
          (holder.type === "file" || holder.type === "image") &&
          key === (holder.type === "image" ? "image" : "data")
        ) {
          const inline = readInlineFile(holder);
          if (inline) return inline.reference;
        }
        if (typeof value === "string")
          return normalizeText(mapString(value), format);
        const bytes = holder?.[key];
        if (
          bytes instanceof Uint8Array &&
          typeof holder?.mediaType === "string"
        ) {
          const reference =
            readInlineFile({
              type: "file",
              data: bytes,
              mediaType: holder.mediaType,
            })?.reference ??
            fileReference({ bytes, mediaType: holder.mediaType });
          onFileContent?.(reference);
          return reference;
        }
        return value;
      },
    );
    return createHash("sha256")
      .update(text ?? "undefined")
      .digest("hex");
  } catch {
    return "unavailable";
  }
}
