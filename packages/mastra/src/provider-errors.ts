const MAX_ERROR_NAME_LENGTH = 80;
const ERROR_NAME = /^[A-Za-z][A-Za-z0-9_]*Error$/;
const STATUS_CATEGORIES: Readonly<Record<number, string>> = {
  400: "invalid request",
  401: "authentication failed",
  403: "permission denied",
  404: "not found",
  408: "timed out",
  413: "request too large",
  422: "invalid request",
  429: "rate limited",
};

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

function getStatusCategory(status: number): string {
  return (
    STATUS_CATEGORIES[status] ??
    (status >= 500 ? "provider unavailable" : "request rejected")
  );
}

/**
 * Describe a failed provider call by class, HTTP status, and status category.
 *
 * The provider's message is left out: providers echo request content and
 * credentials in it in shapes no redaction pattern list covers, and the
 * status category still tells a rate limit, an outage, and a bad key apart.
 * An error without an HTTP status is described by its class name alone.
 */
export function describeProviderError(error: unknown): string | undefined {
  const name = getErrorName(error);
  const status = getHttpStatus(error);
  if (status === undefined) return name;
  return `${name === undefined ? "" : `${name}, `}HTTP ${status}: ${getStatusCategory(status)}`;
}
