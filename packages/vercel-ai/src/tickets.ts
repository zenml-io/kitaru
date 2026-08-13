export const TICKET_WAIT_TIMEOUT_MS = 30_000;

interface Ticket {
  activate(): void;
  predecessorActive: Promise<void>;
  reject(error: unknown): void;
  resolve(): void;
  turn: Promise<void>;
}

function abortError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Operation aborted", "AbortError");
}

async function waitForTurn(
  ticket: Ticket,
  abortSignal: AbortSignal | undefined,
  timeoutMs: number,
): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(
      () =>
        reject(
          new Error(
            `Replay tool ticket timed out after ${timeoutMs}ms waiting to start`,
          ),
        ),
      timeoutMs,
    );
  });
  // The timeout guards a predecessor that never starts, not one that is slow:
  // a legitimately long tool must not fail the calls queued behind it.
  void ticket.predecessorActive.then(
    () => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
        timeout = undefined;
      }
    },
    () => undefined,
  );
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
    await Promise.race([ticket.turn, timeoutPromise, abortPromise]);
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
    let predecessorActive = Promise.resolve();
    for (const callId of callIds) {
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      let activate!: () => void;
      const completion = new Promise<void>((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
      });
      const activation = new Promise<void>((resolveActivation) => {
        activate = resolveActivation;
      });
      void completion.catch(() => undefined);
      this.#tickets.set(callId, {
        activate,
        predecessorActive,
        reject,
        resolve,
        turn: predecessor,
      });
      predecessor = completion;
      predecessorActive = activation;
    }
  }

  async run<T>(
    callId: string,
    execute: () => PromiseLike<T> | T,
    abortSignal?: AbortSignal,
    blocksFollowing: (error: unknown) => boolean = () => true,
    storedFailure: () => unknown = () => undefined,
  ): Promise<T> {
    const ticket = this.#tickets.get(callId);
    if (!ticket) {
      throw new Error(`No replay ticket for tool call '${callId}'`);
    }
    this.#tickets.delete(callId);
    try {
      await waitForTurn(ticket, abortSignal, this.#timeoutMs);
      // Rethrowing the stored failure keeps one error identity for callers:
      // the same value core's decideToolCall throws once a policy has failed.
      const failure = storedFailure();
      if (failure !== undefined) {
        throw failure;
      }
      ticket.activate();
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
