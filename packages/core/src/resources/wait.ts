import { KitaruApiError, KitaruWaitError } from "../errors.js";
import type { ResourceRequestOptions } from "./pagination.js";

const DEFAULT_INTERVAL_MS = 2_000;
const DEFAULT_TIMEOUT_MS = 300_000;
const TERMINAL_EXECUTION_STATUSES = new Set([
  "completed",
  "failed",
  "canceled",
]);

export interface WaitOptions {
  intervalMs?: number;
  signal?: AbortSignal;
  timeoutMs?: number;
}

interface WaitForTerminalOptions<T extends { status: string }> {
  get: (options: ResourceRequestOptions) => Promise<T>;
  options?: WaitOptions;
  resource: string;
  resourceId: string;
}

function requirePositiveFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new TypeError(`${name} must be positive and finite`);
  }
}

function throwWaitError<T>(
  resource: string,
  resourceId: string,
  kind: "canceled" | "timeout",
  lastState: T | undefined,
  cause?: unknown,
): never {
  throw new KitaruWaitError(resource, resourceId, {
    cause,
    kind,
    lastState,
  });
}

async function delay(
  milliseconds: number,
  signal: AbortSignal | undefined,
): Promise<void> {
  if (signal?.aborted) {
    throw signal.reason;
  }
  await new Promise<void>((resolve, reject) => {
    const complete = () => {
      signal?.removeEventListener("abort", cancel);
      resolve();
    };
    const timeout = setTimeout(complete, milliseconds);
    const cancel = () => {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", cancel);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) {
      cancel();
    }
  });
}

export async function waitForTerminal<T extends { status: string }>({
  get,
  options = {},
  resource,
  resourceId,
}: WaitForTerminalOptions<T>): Promise<T> {
  const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  requirePositiveFinite(intervalMs, "intervalMs");
  requirePositiveFinite(timeoutMs, "timeoutMs");

  const deadline = performance.now() + timeoutMs;
  let lastState: T | undefined;
  while (true) {
    if (options.signal?.aborted) {
      throwWaitError(
        resource,
        resourceId,
        "canceled",
        lastState,
        options.signal.reason,
      );
    }
    const remaining = deadline - performance.now();
    if (remaining <= 0) {
      throwWaitError(resource, resourceId, "timeout", lastState);
    }
    const requestSignal =
      options.signal === undefined
        ? AbortSignal.timeout(Math.ceil(remaining))
        : AbortSignal.any([
            options.signal,
            AbortSignal.timeout(Math.ceil(remaining)),
          ]);
    try {
      lastState = await get({
        signal: requestSignal,
      });
    } catch (error) {
      if (options.signal?.aborted) {
        throwWaitError(resource, resourceId, "canceled", lastState, error);
      }
      if (
        requestSignal.aborted ||
        (error instanceof KitaruApiError && error.kind === "timeout")
      ) {
        throwWaitError(resource, resourceId, "timeout", lastState, error);
      }
      throw error;
    }
    if (TERMINAL_EXECUTION_STATUSES.has(lastState.status)) {
      return lastState;
    }

    const sleepRemaining = deadline - performance.now();
    if (sleepRemaining <= 0) {
      throwWaitError(resource, resourceId, "timeout", lastState);
    }
    try {
      await delay(Math.min(intervalMs, sleepRemaining), options.signal);
    } catch (error) {
      throwWaitError(resource, resourceId, "canceled", lastState, error);
    }
  }
}
