import { redactUrlCredentials } from "@zenml-io/kitaru/adapter";

const MAX_ERROR_NAME_LENGTH = 80;
const MAX_PROVIDER_MESSAGE_LENGTH = 500;
const ERROR_NAME = /^[A-Za-z][A-Za-z0-9_]*Error$/;
// Providers echo the credential a request carried in some auth failures, and
// OpenAI-style keys start with a short prefix and a dash.
const AUTHORIZATION_VALUE = /\b(Bearer|Basic)\s+[^\s,;"'`]+/gi;
const PREFIXED_KEY = /\b(?:sk|pk|rk)[-_][A-Za-z0-9_*-]{8,}/g;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** Return an error's class name when it is bounded and more specific than `Error`. */
export function getErrorName(error: unknown): string | undefined {
  return error instanceof Error &&
    error.name !== "Error" &&
    error.name.length <= MAX_ERROR_NAME_LENGTH &&
    ERROR_NAME.test(error.name)
    ? error.name
    : undefined;
}

/**
 * Find the HTTP status a provider answered with.
 *
 * The AI SDK reports it on `APICallError`, and its `RetryError` keeps the
 * last attempt's error in `lastError`.
 */
function getHttpStatus(error: unknown): number | undefined {
  for (const candidate of [
    error,
    isRecord(error) ? error.lastError : undefined,
    isRecord(error) ? error.cause : undefined,
  ]) {
    const status = isRecord(candidate) ? candidate.statusCode : undefined;
    if (
      typeof status === "number" &&
      Number.isInteger(status) &&
      status >= 100 &&
      status <= 599
    )
      return status;
  }
  return undefined;
}

function redactProviderMessage(message: string): string {
  const redacted = redactUrlCredentials(message)
    .replace(AUTHORIZATION_VALUE, "$1 REDACTED")
    .replace(PREFIXED_KEY, "REDACTED");
  return redacted.length > MAX_PROVIDER_MESSAGE_LENGTH
    ? `${redacted.slice(0, MAX_PROVIDER_MESSAGE_LENGTH)}...`
    : redacted;
}

/**
 * Describe a failed provider call by class, HTTP status, and message.
 *
 * AI SDK errors and errors carrying an HTTP status keep a bounded message
 * with credentials redacted, so a rate limit, an outage, and a bad key read
 * differently. Any other error is described by its class name alone,
 * because application errors can carry arbitrary private text.
 */
export function describeProviderError(error: unknown): string | undefined {
  const name = getErrorName(error);
  const status = getHttpStatus(error);
  if (!(error instanceof Error) || (!name?.startsWith("AI_") && !status))
    return name;
  const label = [name, status === undefined ? undefined : `HTTP ${status}`]
    .filter((part) => part !== undefined)
    .join(", ");
  const message = error.message ? redactProviderMessage(error.message) : "";
  return message ? `${label}: ${message}` : label;
}
