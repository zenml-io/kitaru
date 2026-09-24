import { AsyncLocalStorage } from "node:async_hooks";

/** A clock that runs from a recorded turn's start time at wall-clock speed. */
export interface RecordedClock {
  /** The current time on the recorded turn's clock. */
  now(): Date;
  /** Run `task` with `Date.now()` reporting the recorded turn's clock. */
  run<T>(task: () => Promise<T>): Promise<T>;
}

const offsets = new AsyncLocalStorage<number>();

interface DatePatch {
  target: DateConstructor;
  original: () => number;
  shifted: () => number;
  users: number;
}

let patch: DatePatch | undefined;

function readWallClock(): number {
  return patch ? patch.original.call(patch.target) : Date.now();
}

/**
 * Shift `Date.now()` for code running inside `offsets` until released.
 *
 * Mastra compares `Date.now()` with recorded message times and offers no clock
 * option for it. The shift applies only inside a recorded clock's `run`, so
 * other work in the process keeps reading the wall clock, and the original
 * function returns once no recorded clock is running.
 */
function acquireDatePatch(): () => void {
  if (!patch) {
    const target = Date;
    const original = target.now;
    const shifted = () => {
      const offset = offsets.getStore();
      const now = original.call(target);
      return offset === undefined ? now : now - offset;
    };
    target.now = shifted;
    patch = { target, original, shifted, users: 0 };
  }
  const active = patch;
  active.users += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    active.users -= 1;
    if (active.users > 0 || patch !== active) return;
    // Leave a later replacement, such as test fake timers, in place.
    if (active.target.now === active.shifted)
      active.target.now = active.original;
    patch = undefined;
  };
}

/** Create a clock that starts at `startedAt` now and advances in real time. */
export function createRecordedClock(startedAt: Date): RecordedClock {
  const offset = readWallClock() - startedAt.getTime();
  return {
    now: () => new Date(readWallClock() - offset),
    async run<T>(task: () => Promise<T>): Promise<T> {
      const release = acquireDatePatch();
      try {
        return await offsets.run(offset, task);
      } finally {
        release();
      }
    },
  };
}
