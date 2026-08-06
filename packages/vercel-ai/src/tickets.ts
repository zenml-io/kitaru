export const TICKET_WAIT_TIMEOUT_MS = 30_000;

interface Ticket {
  reject(error: unknown): void;
  resolve(): void;
  turn: Promise<void>;
}

function abortError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Operation aborted", "AbortError");
}

async function waitForTurn(
  turn: Promise<void>,
  abortSignal: AbortSignal | undefined,
  timeoutMs: number,
): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(
      () =>
        reject(new Error(`Replay tool ticket timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  const abortPromise = new Promise<never>((_resolve, reject) => {
    if (!abortSignal) {
      return;
    }
    if (abortSignal.aborted) {
      reject(abortError(abortSignal));
      return;
    }
    onAbort = () => reject(abortError(abortSignal));
    abortSignal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    await Promise.race([turn, timeoutPromise, abortPromise]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
    if (abortSignal && onAbort) {
      abortSignal.removeEventListener("abort", onAbort);
    }
  }
}

export class ExecutionTickets {
  readonly #tickets = new Map<string, Ticket>();
  readonly #timeoutMs: number;

  constructor(timeoutMs = TICKET_WAIT_TIMEOUT_MS) {
    this.#timeoutMs = timeoutMs;
  }

  register(callIds: readonly string[]): void {
    const incoming = new Set<string>();
    for (const callId of callIds) {
      if (incoming.has(callId) || this.#tickets.has(callId)) {
        throw new Error(`Duplicate replay tool call ID '${callId}'`);
      }
      incoming.add(callId);
    }

    let predecessor = Promise.resolve();
    for (const callId of callIds) {
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      const completion = new Promise<void>((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
      });
      void completion.catch(() => undefined);
      this.#tickets.set(callId, { reject, resolve, turn: predecessor });
      predecessor = completion;
    }
  }

  async run<T>(
    callId: string,
    execute: () => PromiseLike<T> | T,
    abortSignal?: AbortSignal,
    blocksFollowing: (error: unknown) => boolean = () => true,
    hasStoredFailure: () => boolean = () => false,
  ): Promise<T> {
    const ticket = this.#tickets.get(callId);
    if (!ticket) {
      throw new Error(`No replay ticket for tool call '${callId}'`);
    }
    this.#tickets.delete(callId);
    try {
      await waitForTurn(ticket.turn, abortSignal, this.#timeoutMs);
      if (hasStoredFailure()) {
        throw new Error("Replay execution stopped after an adapter failure");
      }
      const result = await execute();
      ticket.resolve();
      return result;
    } catch (error) {
      if (blocksFollowing(error)) {
        ticket.reject(error);
      } else {
        ticket.resolve();
      }
      throw error;
    }
  }

  assertConsumed(): void {
    if (this.#tickets.size > 0) {
      throw new Error(
        `Replay tool calls were not executed: ${[...this.#tickets.keys()].join(", ")}`,
      );
    }
  }

  cancel(error: unknown): void {
    for (const ticket of this.#tickets.values()) {
      ticket.reject(error);
    }
    this.#tickets.clear();
  }
}
