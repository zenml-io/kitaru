import { createHash, randomUUID } from "node:crypto";
import {
  closeSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import type {
  MastraExclusiveMemoryAccess,
  MastraMemoryLease,
  MastraMemoryLeaseOptions,
  MastraMemorySelector,
} from "../../src/memory-binding.js";

const pause = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

const sleeper = new Int32Array(new SharedArrayBuffer(4));
/** Block the thread, so no timer of this process runs in between. */
const pauseSync = (ms: number) => Atomics.wait(sleeper, 0, 0, ms);

function isExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "EEXIST"
  );
}

/**
 * Test-only shared lease: atomic thread/resource coordination and poison.
 *
 * Every critical section runs synchronously, so a lease call answers before
 * any timer of this process fires. Kitaru bounds each lease call by a
 * wall-clock deadline and treats a late answer as a coordination failure;
 * asynchronous file I/O under CPU load would sometimes miss it and make the
 * lease semantics these tests assert depend on machine speed.
 */
export function createFileMemoryAccess(
  root: string,
): MastraExclusiveMemoryAccess & {
  simulateLeaseLoss(selector: MastraMemorySelector): Promise<void>;
} {
  function directory(kind: "thread" | "resource", id: string): string {
    return join(
      root,
      `${kind}-${createHash("sha256").update(id).digest("hex")}`,
    );
  }
  const globalPoison = join(root, "unknown-writer-poison");

  function withMutexes<T>(mutexes: string[], run: () => T): T {
    const held: string[] = [];
    const deadline = Date.now() + 1000;
    try {
      for (const mutex of mutexes) {
        while (true) {
          try {
            mkdirSync(mutex);
            held.push(mutex);
            break;
          } catch (error) {
            if (!isExists(error) || Date.now() >= deadline) throw error;
            pauseSync(2);
          }
        }
      }
      return run();
    } finally {
      for (const mutex of held.reverse())
        rmSync(mutex, { recursive: true, force: true });
    }
  }

  function withGlobalMutex<T>(run: () => T): T {
    mkdirSync(root, { recursive: true });
    return withMutexes([join(root, "coordination-mutex")], run);
  }

  function withScopes<T>(
    selector: MastraMemorySelector,
    run: (dirs: string[]) => T,
  ): T {
    return withGlobalMutex(() => {
      const dirs = [
        directory("thread", selector.threadId),
        directory("resource", selector.resourceId),
      ].sort();
      for (const dir of dirs) mkdirSync(dir, { recursive: true });
      // All processes acquire both scope locks in the same order. The global
      // mutex also makes unknown-selector poison and reset atomic with them.
      return withMutexes(
        dirs.map((dir) => join(dir, "mutex")),
        () => run(dirs),
      );
    });
  }

  function exists(path: string): boolean {
    try {
      readFileSync(path);
      return true;
    } catch (error) {
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "ENOENT"
      )
        return false;
      throw error;
    }
  }

  function poison(dir: string, persistent: boolean): void {
    const marker = join(dir, persistent ? "persistent-loss" : "poison");
    try {
      closeSync(openSync(marker, "wx"));
    } catch (error) {
      if (!isExists(error)) throw error;
    }
    if (persistent) poison(dir, false);
  }

  function readOptional(path: string): string | undefined {
    return exists(path) ? readFileSync(path, "utf8") : undefined;
  }

  function activeTurns(dir: string): string[] {
    const names = readdirSync(dir);
    return names.filter((name) => name.startsWith("turn-"));
  }

  function makeLease(
    selector: MastraMemorySelector,
    token: string,
    owns: boolean,
    overlapsFinalizingTurn = false,
  ): MastraMemoryLease {
    let released = false;
    const release = async () => {
      if (released) return;
      withScopes(selector, (dirs) => {
        for (const dir of dirs) {
          if (owns) {
            if (readOptional(join(dir, "owner")) === token) {
              rmSync(join(dir, "owner"), { force: true });
              rmSync(join(dir, "finalizing"), { force: true });
            }
          } else {
            rmSync(join(dir, `turn-${token}`), { force: true });
          }
          if (
            !exists(join(dir, "owner")) &&
            activeTurns(dir).length === 0 &&
            !exists(join(dir, "persistent-loss"))
          )
            rmSync(join(dir, "poison"), { force: true });
        }
      });
      released = true;
    };
    return Object.assign(release, {
      async verifyEligibility() {
        if (released || !owns) return false;
        return withScopes(selector, (dirs) => {
          if (exists(globalPoison)) return false;
          for (const dir of dirs) {
            if (exists(join(dir, "poison"))) return false;
            if (readFileSync(join(dir, "owner"), "utf8") !== token)
              return false;
          }
          return true;
        });
      },
      async markFinalizing() {
        // A turn that does not own the selectors is already ineligible, so a
        // later cooperative turn follows it whether or not it is finalizing.
        if (released || !owns) return;
        withScopes(selector, (dirs) => {
          for (const dir of dirs)
            if (readFileSync(join(dir, "owner"), "utf8") === token)
              writeFileSync(join(dir, "finalizing"), token);
        });
      },
      overlapsFinalizingTurn,
    });
  }

  /**
   * Whether no eligible owner is still answering. Turns that do not own a
   * selector are never eligible.
   */
  function followsFinalizingHolder(dirs: string[]): boolean {
    for (const dir of dirs) {
      const owner = readOptional(join(dir, "owner"));
      if (owner === undefined) continue;
      if (readOptional(join(dir, "finalizing")) !== owner) return false;
    }
    return true;
  }

  function isPoisoned(dirs: string[]): boolean {
    return (
      exists(globalPoison) || dirs.some((dir) => exists(join(dir, "poison")))
    );
  }

  return {
    async acquire(selector, options: MastraMemoryLeaseOptions = {}) {
      const waitMs = options.waitMs ?? 100;
      const deadline = Date.now() + waitMs;
      const token = randomUUID();
      while (true) {
        if (options.signal?.aborted)
          throw new Error("Source-thread lease wait cancelled.");
        const status = withScopes(selector, (dirs) => {
          if (isPoisoned(dirs)) {
            for (const dir of dirs)
              writeFileSync(join(dir, `turn-${token}`), "");
            return "denied";
          }
          // Turns overlapping a finalizing owner hold the selectors without
          // poison, so they keep a later turn out as well.
          for (const dir of dirs)
            if (exists(join(dir, "owner")) || activeTurns(dir).length > 0)
              return "busy";
          for (const dir of dirs)
            writeFileSync(join(dir, "owner"), token, { flag: "wx" });
          return "owned";
        });
        if (status === "owned") return makeLease(selector, token, true);
        if (status === "denied") return makeLease(selector, token, false);
        if (Date.now() >= deadline) {
          const follows = withScopes(selector, (dirs) => {
            // Poison exists only after an invalidating overlap, so without it
            // every non-owner turn already follows a finalizing holder.
            const follows =
              options.cooperative === true &&
              !isPoisoned(dirs) &&
              followsFinalizingHolder(dirs);
            for (const dir of dirs) {
              if (!follows) poison(dir, false);
              writeFileSync(join(dir, `turn-${token}`), "");
            }
            return follows;
          });
          return makeLease(selector, token, false, follows);
        }
        await pause(Math.min(5, deadline - Date.now()));
      }
    },
    async markUnsafeWrite(selector) {
      if (!selector) {
        withGlobalMutex(() => writeFileSync(globalPoison, "unsafe write"));
        return;
      }
      withScopes(selector, (dirs) => {
        for (const dir of dirs) poison(dir, true);
      });
    },
    async resetAfterQuiescence(selector) {
      if (!selector) {
        withGlobalMutex(() => {
          for (const name of readdirSync(root)) {
            if (!name.startsWith("thread-") && !name.startsWith("resource-"))
              continue;
            const dir = join(root, name);
            if (exists(join(dir, "owner")) || activeTurns(dir).length > 0)
              throw new Error("Source-thread writers are still active.");
          }
          rmSync(globalPoison, { force: true });
        });
        return;
      }
      withScopes(selector, (dirs) => {
        for (const dir of dirs) {
          if (exists(join(dir, "owner")) || activeTurns(dir).length > 0)
            throw new Error("Source-thread writers are still active.");
        }
        for (const dir of dirs) {
          rmSync(join(dir, "persistent-loss"), { force: true });
          rmSync(join(dir, "poison"), { force: true });
        }
      });
    },
    async simulateLeaseLoss(selector) {
      withScopes(selector, (dirs) => {
        for (const dir of dirs) {
          poison(dir, true);
          rmSync(join(dir, "owner"), { force: true });
        }
      });
    },
  };
}
