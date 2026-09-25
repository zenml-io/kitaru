import { createHash, randomUUID } from "node:crypto";
import {
  mkdir,
  open,
  readdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import type {
  MastraExclusiveMemoryAccess,
  MastraMemoryLease,
  MastraMemoryLeaseOptions,
  MastraMemorySelector,
} from "../../src/memory-binding.js";

const pause = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

function isExists(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "EEXIST"
  );
}

/** Test-only shared lease: atomic thread/resource coordination and poison. */
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

  async function withMutexes<T>(
    mutexes: string[],
    run: () => Promise<T>,
  ): Promise<T> {
    const held: string[] = [];
    const deadline = Date.now() + 1000;
    try {
      for (const mutex of mutexes) {
        while (true) {
          try {
            await mkdir(mutex);
            held.push(mutex);
            break;
          } catch (error) {
            if (!isExists(error) || Date.now() >= deadline) throw error;
            await pause(2);
          }
        }
      }
      return await run();
    } finally {
      for (const mutex of held.reverse())
        await rm(mutex, { recursive: true, force: true });
    }
  }

  async function withGlobalMutex<T>(run: () => Promise<T>): Promise<T> {
    await mkdir(root, { recursive: true });
    return withMutexes([join(root, "coordination-mutex")], run);
  }

  async function withScopes<T>(
    selector: MastraMemorySelector,
    run: (dirs: string[]) => Promise<T>,
  ): Promise<T> {
    return withGlobalMutex(async () => {
      const dirs = [
        directory("thread", selector.threadId),
        directory("resource", selector.resourceId),
      ].sort();
      for (const dir of dirs) await mkdir(dir, { recursive: true });
      // All processes acquire both scope locks in the same order. The global
      // mutex also makes unknown-selector poison and reset atomic with them.
      return withMutexes(
        dirs.map((dir) => join(dir, "mutex")),
        () => run(dirs),
      );
    });
  }

  async function exists(path: string): Promise<boolean> {
    try {
      await readFile(path);
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

  async function poison(dir: string, persistent: boolean): Promise<void> {
    const marker = join(dir, persistent ? "persistent-loss" : "poison");
    try {
      const file = await open(marker, "wx");
      await file.close();
    } catch (error) {
      if (!isExists(error)) throw error;
    }
    if (persistent) await poison(dir, false);
  }

  async function activeTurns(dir: string): Promise<string[]> {
    const names = await readdir(dir);
    return names.filter((name) => name.startsWith("turn-"));
  }

  async function makeLease(
    selector: MastraMemorySelector,
    token: string,
    owns: boolean,
    overlapsFinalizingTurn = false,
  ): Promise<MastraMemoryLease> {
    let released = false;
    const release = async () => {
      if (released) return;
      await withScopes(selector, async (dirs) => {
        for (const dir of dirs) {
          if (owns) {
            const current = await readFile(join(dir, "owner"), "utf8").catch(
              () => undefined,
            );
            if (current === token) {
              await rm(join(dir, "owner"), { force: true });
              await rm(join(dir, "finalizing"), { force: true });
            }
          } else {
            await rm(join(dir, `turn-${token}`), { force: true });
            await rm(join(dir, `finalizing-${token}`), { force: true });
          }
          if (
            !(await exists(join(dir, "owner"))) &&
            (await activeTurns(dir)).length === 0 &&
            !(await exists(join(dir, "persistent-loss")))
          )
            await rm(join(dir, "poison"), { force: true });
        }
      });
      released = true;
    };
    return Object.assign(release, {
      async verifyEligibility() {
        if (released || !owns) return false;
        return withScopes(selector, async (dirs) => {
          if (await exists(globalPoison)) return false;
          for (const dir of dirs) {
            if (await exists(join(dir, "poison"))) return false;
            if ((await readFile(join(dir, "owner"), "utf8")) !== token)
              return false;
          }
          return true;
        });
      },
      async markFinalizing() {
        if (released) return;
        await withScopes(selector, async (dirs) => {
          for (const dir of dirs) {
            if (!owns) {
              if (await exists(join(dir, `turn-${token}`)))
                await writeFile(join(dir, `finalizing-${token}`), "");
            } else if ((await readFile(join(dir, "owner"), "utf8")) === token)
              await writeFile(join(dir, "finalizing"), token);
          }
        });
      },
      overlapsFinalizingTurn,
    });
  }

  /**
   * Whether some holder is finalizing and no eligible owner is not. Turns
   * that do not own a selector are never eligible.
   */
  async function followsFinalizingHolder(dirs: string[]): Promise<boolean> {
    let finalizing = false;
    for (const dir of dirs) {
      const owner = await readFile(join(dir, "owner"), "utf8").catch(
        () => undefined,
      );
      if ((await readdir(dir)).some((name) => name.startsWith("finalizing-")))
        finalizing = true;
      if (owner === undefined) continue;
      const marked = await readFile(join(dir, "finalizing"), "utf8").catch(
        () => undefined,
      );
      if (marked !== owner) return false;
      finalizing = true;
    }
    return finalizing;
  }

  return {
    async acquire(selector, options: MastraMemoryLeaseOptions = {}) {
      const waitMs = options.waitMs ?? 100;
      const deadline = Date.now() + waitMs;
      const token = randomUUID();
      while (true) {
        if (options.signal?.aborted)
          throw new Error("Source-thread lease wait cancelled.");
        const status = await withScopes(selector, async (dirs) => {
          if (
            (await exists(globalPoison)) ||
            (
              await Promise.all(dirs.map((dir) => exists(join(dir, "poison"))))
            ).some(Boolean)
          ) {
            for (const dir of dirs)
              await writeFile(join(dir, `turn-${token}`), "");
            return "denied";
          }
          // Turns overlapping a finalizing owner hold the selectors without
          // poison, so they keep a later turn out as well.
          for (const dir of dirs)
            if (
              (await exists(join(dir, "owner"))) ||
              (await activeTurns(dir)).length > 0
            )
              return "busy";
          for (const dir of dirs)
            await writeFile(join(dir, "owner"), token, { flag: "wx" });
          return "owned";
        });
        if (status === "owned") return makeLease(selector, token, true);
        if (status === "denied") return makeLease(selector, token, false);
        if (Date.now() >= deadline) {
          const follows = await withScopes(selector, async (dirs) => {
            // Poison exists only after an invalidating overlap, so without it
            // every non-owner turn already follows a finalizing holder.
            const follows =
              options.cooperative === true &&
              !(await exists(globalPoison)) &&
              !(
                await Promise.all(
                  dirs.map((dir) => exists(join(dir, "poison"))),
                )
              ).some(Boolean) &&
              (await followsFinalizingHolder(dirs));
            for (const dir of dirs) {
              if (!follows) await poison(dir, false);
              await writeFile(join(dir, `turn-${token}`), "");
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
        await withGlobalMutex(() => writeFile(globalPoison, "unsafe write"));
        return;
      }
      await withScopes(selector, async (dirs) => {
        for (const dir of dirs) await poison(dir, true);
      });
    },
    async resetAfterQuiescence(selector) {
      if (!selector) {
        await withGlobalMutex(async () => {
          for (const name of await readdir(root)) {
            if (!name.startsWith("thread-") && !name.startsWith("resource-"))
              continue;
            const dir = join(root, name);
            if (
              (await exists(join(dir, "owner"))) ||
              (await activeTurns(dir)).length > 0
            )
              throw new Error("Source-thread writers are still active.");
          }
          await rm(globalPoison, { force: true });
        });
        return;
      }
      await withScopes(selector, async (dirs) => {
        for (const dir of dirs) {
          if (
            (await exists(join(dir, "owner"))) ||
            (await activeTurns(dir)).length > 0
          )
            throw new Error("Source-thread writers are still active.");
        }
        for (const dir of dirs) {
          await rm(join(dir, "persistent-loss"), { force: true });
          await rm(join(dir, "poison"), { force: true });
        }
      });
    },
    async simulateLeaseLoss(selector) {
      await withScopes(selector, async (dirs) => {
        for (const dir of dirs) {
          await poison(dir, true);
          await rm(join(dir, "owner"), { force: true });
        }
      });
    },
  };
}
