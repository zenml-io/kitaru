import {
  bindCredentialProvider,
  type CredentialProvider,
  isRenewableCredentialProvider,
  KitaruCredentialError,
  type ResolvedCredential,
} from "./auth/index.js";
import { KitaruApiError, type KitaruApiErrorKind } from "./errors.js";
import { encodeQuery, type QueryParameters } from "./query.js";
import { isRecord } from "./validation.js";

export type HttpMethod = "DELETE" | "GET" | "PATCH" | "POST" | "PUT";
export type ResponseType = "bytes" | "empty" | "json";
export type { CredentialProvider } from "./auth/index.js";

export interface RequestBody {
  create(): { body: BodyInit; contentType?: string };
}

export type FormValue =
  | string
  | number
  | boolean
  | readonly (string | number | boolean)[]
  | null
  | undefined;

export interface MultipartPart {
  name: string;
  value: string | Blob | ArrayBuffer | Uint8Array;
  filename?: string;
  contentType?: string;
}

type BufferedMultipartPart = Omit<MultipartPart, "value"> & {
  value: string | Blob;
};

export interface RetryPolicy {
  attempts: number;
  retryTransportErrors?: boolean;
  statuses?: ReadonlySet<number>;
}

export type ResponseValidator<T> = (
  value: unknown,
  method: HttpMethod,
  path: string,
  status: number,
) => asserts value is T;

export interface TransportRequest<T = unknown> {
  authenticate?: boolean;
  method: HttpMethod;
  path: string;
  body?: RequestBody;
  headers?: Readonly<Record<string, string>>;
  query?: QueryParameters;
  responseType?: ResponseType;
  retry?: RetryPolicy;
  signal?: AbortSignal;
  validate?: ResponseValidator<T>;
}

export interface KitaruTransportOptions {
  apiUrl: string;
  credentialProvider?: CredentialProvider;
  fetch?: typeof globalThis.fetch;
  timeoutMs: number;
}

const KITARU_CLIENT_HEADER = "X-Kitaru-Client";
const KITARU_CLIENT_IDENTIFICATION = "kitaru-typescript";
const KITARU_SKILL_HEADER = "X-Kitaru-Skill";
const IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
const IDEMPOTENT_METHODS: ReadonlySet<string> = new Set(["POST"]);

function cloneBytes(value: ArrayBuffer | Uint8Array): Uint8Array<ArrayBuffer> {
  const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

export function jsonBody(value: unknown): RequestBody {
  const body = JSON.stringify(value);
  return {
    create: () => ({
      body,
      contentType: "application/json",
    }),
  };
}

export function formBody(
  values: Readonly<Record<string, FormValue>>,
): RequestBody {
  return {
    create: () => {
      const body = new URLSearchParams();
      for (const [name, value] of Object.entries(values)) {
        if (value === undefined || value === null) {
          continue;
        }
        const items = Array.isArray(value) ? value : [value];
        for (const item of items) {
          body.append(name, String(item));
        }
      }
      return {
        body,
        contentType: "application/x-www-form-urlencoded;charset=UTF-8",
      };
    },
  };
}

export function bytesBody(
  value: ArrayBuffer | Uint8Array,
  contentType = "application/octet-stream",
): RequestBody {
  const buffered = cloneBytes(value);
  return {
    create: () => ({ body: cloneBytes(buffered).buffer, contentType }),
  };
}

export function multipartBody(parts: readonly MultipartPart[]): RequestBody {
  const buffered = parts.map<BufferedMultipartPart>((part) => {
    if (typeof part.value === "string") {
      return { ...part, value: part.value };
    }
    const value =
      part.value instanceof Blob
        ? part.contentType === undefined || part.contentType === part.value.type
          ? part.value
          : new Blob([part.value], { type: part.contentType })
        : new Blob([cloneBytes(part.value).buffer], {
            type: part.contentType,
          });
    return { ...part, value };
  });
  return {
    create: () => {
      const body = new FormData();
      for (const part of buffered) {
        if (typeof part.value === "string") {
          body.append(part.name, part.value);
          continue;
        }
        if (part.filename === undefined) {
          body.append(part.name, part.value);
        } else {
          body.append(part.name, part.value, part.filename);
        }
      }
      return { body };
    },
  };
}

function isLoopback(hostname: string): boolean {
  const normalized = hostname.toLowerCase();
  if (
    normalized === "localhost" ||
    normalized === "[::1]" ||
    normalized === "::1"
  ) {
    return true;
  }
  const match = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(normalized);
  if (match === null) {
    return false;
  }
  return (
    match.slice(1).every((part) => Number(part) <= 255) && match[1] === "127"
  );
}

export function normalizeApiUrl(value: string): string {
  if (value.includes("\\")) {
    throw new Error("apiUrl contains unsafe path normalization");
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch (error) {
    throw new Error("apiUrl must be a valid absolute URL", { cause: error });
  }
  if (
    url.protocol !== "https:" &&
    !(url.protocol === "http:" && isLoopback(url.hostname))
  ) {
    throw new Error("apiUrl must use HTTPS unless it targets loopback");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error("apiUrl must not contain userinfo, query, or fragment");
  }
  const authorityStart = value.indexOf("://") + 3;
  const pathStart = value.indexOf("/", authorityStart);
  const rawPath = pathStart === -1 ? "" : value.slice(pathStart);
  if (/%(?:2e|2f|5c)/i.test(rawPath) || /(?:^|\/)\.\.?(?:\/|$)/.test(rawPath)) {
    throw new Error("apiUrl contains unsafe path normalization");
  }
  return `${url.protocol}//${url.host}${url.pathname.replace(/\/+$/, "")}`;
}

function redact(value: string, secrets: readonly string[]): string {
  return secrets.reduce(
    (result, secret) =>
      secret.length === 0 ? result : result.replaceAll(secret, "[REDACTED]"),
    value,
  );
}

function safeErrorDetail(
  payload: unknown,
  fallback: string,
  secrets: readonly string[],
): string {
  if (!isRecord(payload) || !Object.hasOwn(payload, "detail")) {
    return fallback;
  }
  if (typeof payload.detail === "string") {
    return redact(payload.detail.trim() || fallback, secrets);
  }
  if (!Array.isArray(payload.detail)) {
    return fallback;
  }

  const messages = payload.detail.flatMap((entry) => {
    if (!isRecord(entry) || typeof entry.msg !== "string") {
      return [];
    }
    const location = Array.isArray(entry.loc)
      ? entry.loc.filter((item) => typeof item === "string").join(".")
      : "";
    const message = redact(entry.msg, secrets);
    return [location ? `${location}: ${message}` : message];
  });
  return messages.length > 0 ? messages.join("; ") : fallback;
}

async function decodeJson(response: Response): Promise<unknown> {
  if (response.status === 204 || response.status === 205) {
    return undefined;
  }
  return response.json();
}

async function discardResponse(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Preserve the request error or retry decision when cleanup is unavailable.
  }
}

interface RequestDeadline {
  canceledByCaller: () => boolean;
  cleanup: () => void;
  signal: AbortSignal;
}

function createRequestDeadline(
  callerSignal: AbortSignal | undefined,
  timeoutMs: number,
): RequestDeadline {
  const controller = new AbortController();
  let canceledByCaller = callerSignal?.aborted ?? false;
  const cancel = () => {
    canceledByCaller = true;
    controller.abort(callerSignal?.reason);
  };
  callerSignal?.addEventListener("abort", cancel, { once: true });
  if (canceledByCaller) {
    controller.abort(callerSignal?.reason);
  }
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);
  return {
    canceledByCaller: () => canceledByCaller,
    cleanup: () => {
      clearTimeout(timeout);
      callerSignal?.removeEventListener("abort", cancel);
    },
    signal: controller.signal,
  };
}

export class KitaruTransport {
  readonly #apiUrl: string;
  readonly #credentialProvider?: ReturnType<typeof bindCredentialProvider>;
  readonly #fetch: typeof globalThis.fetch;
  readonly #timeoutMs: number;
  readonly #skill: string | undefined;

  constructor(options: KitaruTransportOptions) {
    this.#apiUrl = normalizeApiUrl(options.apiUrl);
    this.#credentialProvider =
      options.credentialProvider === undefined
        ? undefined
        : bindCredentialProvider(options.credentialProvider);
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#timeoutMs = options.timeoutMs;
    this.#skill =
      typeof process === "undefined"
        ? undefined
        : process.env.KITARU_ACTIVE_SKILL || undefined;
  }

  async request<T>(request: TransportRequest<T>): Promise<T> {
    if (
      !request.path.startsWith("/") ||
      request.path.includes("?") ||
      request.path.includes("#")
    ) {
      throw new Error(
        "Transport paths must start with / and exclude query or fragments",
      );
    }
    const attempts = request.retry?.attempts ?? 1;
    if (!Number.isInteger(attempts) || attempts < 1) {
      throw new Error("Retry attempts must be a positive integer");
    }
    const failForAbort = (
      deadline: RequestDeadline,
      cause?: unknown,
    ): never => {
      const kind: KitaruApiErrorKind = deadline.canceledByCaller()
        ? "canceled"
        : "timeout";
      const detail = deadline.canceledByCaller()
        ? "Request canceled by caller"
        : `Request timed out after ${this.#timeoutMs}ms`;
      throw new KitaruApiError(request.method, request.path, null, detail, {
        cause,
        kind,
      });
    };

    let credential: ResolvedCredential | undefined;
    const provider = this.#credentialProvider;
    if (provider !== undefined && request.authenticate !== false) {
      const credentialDeadline = createRequestDeadline(
        request.signal,
        this.#timeoutMs,
      );
      try {
        if (credentialDeadline.signal.aborted) {
          failForAbort(credentialDeadline);
        }
        try {
          credential = await provider.getCredential(credentialDeadline.signal);
        } catch (error) {
          if (credentialDeadline.signal.aborted) {
            failForAbort(credentialDeadline, error);
          }
          throw new KitaruApiError(
            request.method,
            request.path,
            null,
            error instanceof KitaruCredentialError
              ? error.message
              : "Credential lookup failed",
            { cause: error, kind: "transport" },
          );
        }
      } finally {
        credentialDeadline.cleanup();
      }
    }

    // One key per logical request, reused on every attempt so the server
    // replays the first committed response instead of running it again.
    const method = request.method.toUpperCase();
    const idempotencyKey = IDEMPOTENT_METHODS.has(method)
      ? globalThis.crypto.randomUUID()
      : undefined;
    let attempt = 0;
    let authenticationRetried = false;
    while (attempt < attempts) {
      const attemptDeadline = createRequestDeadline(
        request.signal,
        this.#timeoutMs,
      );
      try {
        if (attemptDeadline.signal.aborted) {
          failForAbort(attemptDeadline);
        }
        const createdBody = request.body?.create();
        let response: Response;
        try {
          response = await this.#fetch(
            `${this.#apiUrl}${request.path}${encodeQuery(request.query)}`,
            {
              method,
              redirect: "manual",
              headers: {
                Accept: "application/json",
                [KITARU_CLIENT_HEADER]: KITARU_CLIENT_IDENTIFICATION,
                ...(this.#skill === undefined
                  ? {}
                  : { [KITARU_SKILL_HEADER]: this.#skill }),
                ...(createdBody?.contentType === undefined
                  ? {}
                  : { "Content-Type": createdBody.contentType }),
                ...(credential === undefined
                  ? {}
                  : { Authorization: `Bearer ${credential.token}` }),
                ...(idempotencyKey === undefined
                  ? {}
                  : { [IDEMPOTENCY_KEY_HEADER]: idempotencyKey }),
                ...request.headers,
              },
              body: createdBody?.body,
              signal: attemptDeadline.signal,
            },
          );
        } catch (error) {
          if (attemptDeadline.signal.aborted) {
            failForAbort(attemptDeadline, error);
          }
          if (request.retry?.retryTransportErrors && attempt + 1 < attempts) {
            attempt += 1;
            continue;
          }
          throw new KitaruApiError(
            request.method,
            request.path,
            null,
            "Request failed",
            { cause: error, kind: "transport" },
          );
        }

        if (response.status >= 300 && response.status < 400) {
          await discardResponse(response);
          throw new KitaruApiError(
            request.method,
            request.path,
            response.status,
            "Authenticated redirects are not allowed",
            { kind: "redirect" },
          );
        }

        if (
          response.status === 401 &&
          !authenticationRetried &&
          credential !== undefined &&
          this.#credentialProvider !== undefined &&
          isRenewableCredentialProvider(this.#credentialProvider)
        ) {
          await discardResponse(response);
          try {
            credential = await this.#credentialProvider.renewCredential(
              credential,
              attemptDeadline.signal,
            );
          } catch (error) {
            if (attemptDeadline.signal.aborted) {
              failForAbort(attemptDeadline, error);
            }
            throw new KitaruApiError(
              request.method,
              request.path,
              401,
              error instanceof KitaruCredentialError
                ? error.message
                : "Credential renewal failed",
              { cause: error },
            );
          }
          authenticationRetried = true;
          continue;
        }
        if (
          !response.ok &&
          attempt + 1 < attempts &&
          request.retry?.statuses?.has(response.status)
        ) {
          await discardResponse(response);
          attempt += 1;
          continue;
        }

        let value: unknown;
        try {
          if (!response.ok || request.responseType !== "bytes") {
            value =
              request.responseType === "empty" && response.ok
                ? undefined
                : await decodeJson(response);
          } else {
            value = new Uint8Array(await response.arrayBuffer());
          }
        } catch (error) {
          if (attemptDeadline.signal.aborted) {
            failForAbort(attemptDeadline, error);
          }
          if (error instanceof SyntaxError) {
            value = undefined;
          } else if (
            request.retry?.retryTransportErrors &&
            attempt + 1 < attempts
          ) {
            await discardResponse(response);
            attempt += 1;
            continue;
          } else {
            throw new KitaruApiError(
              request.method,
              request.path,
              null,
              "Request failed",
              { cause: error, kind: "transport" },
            );
          }
        }

        if (!response.ok) {
          throw new KitaruApiError(
            request.method,
            request.path,
            response.status,
            safeErrorDetail(
              value,
              response.statusText || "Request failed",
              credential === undefined ? [] : [credential.token],
            ),
          );
        }
        request.validate?.(
          value,
          request.method,
          request.path,
          response.status,
        );
        return value as T;
      } finally {
        attemptDeadline.cleanup();
      }
    }
    throw new Error("Unreachable request state");
  }
}
