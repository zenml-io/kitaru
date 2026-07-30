import type { Session } from "../api/types";

const DATE_FORMAT = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

export function formatDate(iso: string | null | undefined): string {
  if (!iso) {
    return "—";
  }
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? iso : DATE_FORMAT.format(date);
}

export function formatDuration(
  start: string | null | undefined,
  end: string | null | undefined,
): string {
  if (!start || !end) {
    return "—";
  }
  const milliseconds = new Date(end).getTime() - new Date(start).getTime();
  if (Number.isNaN(milliseconds) || milliseconds < 0) {
    return "—";
  }
  if (milliseconds < 1_000) {
    return `${milliseconds}ms`;
  }
  const seconds = milliseconds / 1_000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.round(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remainder}s`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

/** Cost is a Decimal serialized as a string — render it verbatim, never parse. */
export function formatCost(cost: string | null | undefined): string {
  if (!cost) {
    return "—";
  }
  return `$${cost}`;
}

export function formatScore(
  score: number | boolean | null | undefined,
): string {
  if (score === null || score === undefined) {
    return "—";
  }
  return String(score);
}

function formatCount(count: number): string {
  if (count >= 1_000_000) {
    return `${(count / 1_000_000).toFixed(1)}M`;
  }
  if (count >= 1_000) {
    return `${(count / 1_000).toFixed(1)}k`;
  }
  return String(count);
}

export function formatTokens(tokens: Session["tokens"]): string {
  if (!tokens) {
    return "—";
  }
  const parts: string[] = [];
  if (tokens.input_tokens != null) {
    parts.push(`${formatCount(tokens.input_tokens)} in`);
  }
  if (tokens.output_tokens != null) {
    parts.push(`${formatCount(tokens.output_tokens)} out`);
  }
  if (tokens.reasoning_tokens != null) {
    parts.push(`${formatCount(tokens.reasoning_tokens)} think`);
  }
  return parts.length > 0 ? parts.join(" / ") : "—";
}

export function shortId(id: string): string {
  return id.slice(0, 8);
}
