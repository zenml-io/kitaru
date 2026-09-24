import type { Memory } from "@mastra/memory";
import type { JsonValue } from "@zenml-io/kitaru";

/** The tokens a recorded turn counted for one captured attachment. */
export interface AttachmentTokenCount {
  /** Mastra's synchronous estimate, from the attachment's descriptor. */
  sync?: number;
  /** Mastra's asynchronous estimate, which can ask the provider to count. */
  async?: number;
}

/** Recorded attachment token counts, keyed by captured file reference. */
export type AttachmentTokenCounts = Record<string, AttachmentTokenCount>;

type OMEngine = NonNullable<Awaited<Memory["omEngine"]>>;

interface AttachmentCounter {
  countAttachmentPartSync(part: unknown): number | undefined;
  countAttachmentPartAsync(part: unknown): Promise<number | undefined>;
}

const FILE_REFERENCE = /^kitaru-file:\/\/sha256\/[a-f0-9]{64}$/;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isAttachmentCounter(value: unknown): value is AttachmentCounter {
  return (
    record(value) &&
    typeof value.countAttachmentPartSync === "function" &&
    typeof value.countAttachmentPartAsync === "function"
  );
}

/** Return the counter Mastra's OM uses, or undefined when its shape changed. */
function getAttachmentCounter(engine: OMEngine): AttachmentCounter | undefined {
  const counter: unknown = engine.getTokenCounter();
  return isAttachmentCounter(counter) ? counter : undefined;
}

/** Return the value a file or image part holds its attachment in. */
function getAttachmentData(part: unknown): unknown {
  if (!record(part)) return undefined;
  const data = part.type === "image" ? part.image : part.data;
  return data instanceof URL ? data.href : data;
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

/**
 * Record the tokens Mastra's observational memory counts for each declared
 * attachment during a baseline turn, without changing any count.
 *
 * Mastra counts an attachment from its URL: the file name in the URL, or the
 * provider's own count of the file behind it. A replay's history holds the
 * captured reference instead, which counts very differently, so replay reuses
 * these counts. `referenceFor` returns a declared URL's captured reference.
 */
export function recordAttachmentTokens(
  engine: OMEngine,
  referenceFor: (value: string) => string | undefined,
): { counts(): AttachmentTokenCounts } {
  const counts: AttachmentTokenCounts = {};
  const counter = getAttachmentCounter(engine);
  if (!counter) return { counts: () => counts };
  const keep = (part: unknown, kind: "sync" | "async", tokens: unknown) => {
    const data = getAttachmentData(part);
    const reference = typeof data === "string" ? referenceFor(data) : undefined;
    if (!reference || !isCount(tokens)) return;
    counts[reference] = { [kind]: tokens, ...counts[reference] };
  };
  const countSync = counter.countAttachmentPartSync.bind(counter);
  const countAsync = counter.countAttachmentPartAsync.bind(counter);
  // Instance properties shadow the prototype, so Mastra's own `this.` calls
  // reach these versions too.
  counter.countAttachmentPartSync = (part) => {
    const tokens = countSync(part);
    keep(part, "sync", tokens);
    return tokens;
  };
  counter.countAttachmentPartAsync = async (part) => {
    const tokens = await countAsync(part);
    keep(part, "async", tokens);
    return tokens;
  };
  return { counts: () => counts };
}

/**
 * Count each captured attachment in a replay as its recorded turn did.
 *
 * An attachment without a recorded asynchronous count takes its synchronous
 * one, because Mastra's asynchronous count would ask the provider live.
 */
export function replayAttachmentTokens(
  engine: OMEngine,
  counts: AttachmentTokenCounts,
): void {
  const counter = getAttachmentCounter(engine);
  if (!counter) return;
  const countSync = counter.countAttachmentPartSync.bind(counter);
  const countAsync = counter.countAttachmentPartAsync.bind(counter);
  const recorded = (part: unknown): AttachmentTokenCount | undefined => {
    const data = getAttachmentData(part);
    return typeof data === "string" && FILE_REFERENCE.test(data)
      ? (counts[data] ?? {})
      : undefined;
  };
  counter.countAttachmentPartSync = (part) => {
    const count = recorded(part);
    return count?.sync ?? countSync(part);
  };
  counter.countAttachmentPartAsync = async (part) => {
    const count = recorded(part);
    if (!count) return countAsync(part);
    return count.async ?? count.sync ?? countSync(part);
  };
}

/** Validate recorded attachment token counts read from a replay input. */
export function readAttachmentTokenCounts(
  value: JsonValue | undefined,
): AttachmentTokenCounts | undefined {
  if (value === undefined) return undefined;
  if (!record(value)) return undefined;
  const counts: AttachmentTokenCounts = {};
  for (const [reference, entry] of Object.entries(value)) {
    if (!FILE_REFERENCE.test(reference) || !record(entry)) return undefined;
    const count: AttachmentTokenCount = {};
    for (const kind of ["sync", "async"] as const) {
      const tokens = entry[kind];
      if (tokens === undefined) continue;
      if (!isCount(tokens)) return undefined;
      count[kind] = tokens;
    }
    counts[reference] = count;
  }
  return counts;
}
