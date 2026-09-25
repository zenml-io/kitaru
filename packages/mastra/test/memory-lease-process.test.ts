import { type ChildProcess, fork } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it } from "vitest";

const workerPath = fileURLToPath(
  new URL("./helpers/file-memory-worker.mjs", import.meta.url),
);
const thread = { threadId: "shared-thread", resourceId: "resource" };
const sameResource = { threadId: "other-thread", resourceId: "resource" };
const sameThread = { threadId: "shared-thread", resourceId: "other-resource" };
const other = { threadId: "other-thread", resourceId: "other-resource" };
const unrelated = {
  threadId: "unrelated-thread",
  resourceId: "unrelated-resource",
};
let root: string | undefined;
const children: ChildProcess[] = [];
let nextId = 0;

type Reply = { id: number; ok: boolean; result?: unknown; error?: string };

async function worker(): Promise<{
  child: ChildProcess;
  request(message: Record<string, unknown>): Promise<unknown>;
  start(message: Record<string, unknown>): {
    id: number;
    result: Promise<unknown>;
  };
}> {
  root ??= await mkdtemp(join(tmpdir(), "kitaru-lease-"));
  const child = fork(workerPath, [], {
    env: { ...process.env, KITARU_LEASE_TEST_ROOT: root },
    execArgv: ["--experimental-strip-types"],
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  children.push(child);
  const pending = new Map<
    number,
    { resolve(value: unknown): void; reject(error: Error): void }
  >();
  child.on("message", (value: Reply) => {
    const waiting = pending.get(value.id);
    if (!waiting) return;
    pending.delete(value.id);
    if (value.ok) waiting.resolve(value.result);
    else waiting.reject(new Error(value.error));
  });
  child.on("exit", (code) => {
    for (const waiting of pending.values())
      waiting.reject(new Error(`Lease worker exited with ${code}`));
    pending.clear();
  });
  function start(message: Record<string, unknown>) {
    const id = ++nextId;
    const result = new Promise<unknown>((resolve, reject) => {
      pending.set(id, { resolve, reject });
      child.send({ id, ...message });
    });
    return { id, result };
  }
  return { child, start, request: (message) => start(message).result };
}

afterEach(async () => {
  for (const child of children.splice(0)) child.kill();
  if (root) await rm(root, { recursive: true, force: true });
  root = undefined;
});

it("invalidates both processes on bounded same-thread contention, then recovers after both finish", async () => {
  const first = await worker();
  const second = await worker();
  expect(
    await first.request({ action: "acquire", name: "first", selector: thread }),
  ).toBe(true);
  expect(
    await second.request({
      action: "acquire",
      name: "second",
      selector: thread,
      waitMs: 40,
    }),
  ).toBe(false);
  expect(
    await first.request({ action: "verify", name: "first", selector: thread }),
  ).toBe(false);
  await Promise.all([
    first.request({ action: "release", name: "first", selector: thread }),
    second.request({ action: "release", name: "second", selector: thread }),
  ]);
  expect(
    await second.request({ action: "acquire", name: "next", selector: thread }),
  ).toBe(true);
  await second.request({ action: "release", name: "next", selector: thread });
});

it.each([
  ["resource", sameResource],
  ["thread", sameThread],
] as const)(
  "invalidates both processes when selectors share a %s",
  async (_scope, competing) => {
    const first = await worker();
    const second = await worker();
    expect(
      await first.request({
        action: "acquire",
        name: "first",
        selector: thread,
      }),
    ).toBe(true);
    expect(
      await second.request({
        action: "acquire",
        name: "competing",
        selector: competing,
        waitMs: 40,
      }),
    ).toBe(false);
    expect(await first.request({ action: "verify", name: "first" })).toBe(
      false,
    );
    // Poison is scoped to the conflicting thread and resource, not all turns.
    expect(
      await second.request({
        action: "acquire",
        name: "other",
        selector: unrelated,
      }),
    ).toBe(true);
    await Promise.all([
      first.request({ action: "release", name: "first" }),
      second.request({ action: "release", name: "competing" }),
      second.request({ action: "release", name: "other" }),
    ]);
    expect(
      await second.request({
        action: "acquire",
        name: "next",
        selector: thread,
      }),
    ).toBe(true);
    await second.request({ action: "release", name: "next" });
  },
);

it("poisons a lost holder before a stale native write and blocks a successor", async () => {
  const first = await worker();
  const second = await worker();
  expect(
    await first.request({ action: "acquire", name: "first", selector: thread }),
  ).toBe(true);
  await second.request({ action: "lose", selector: thread });
  expect(
    await first.request({ action: "verify", name: "first", selector: thread }),
  ).toBe(false);
  await first.request({ action: "nativeWrite", selector: thread });
  expect(
    await second.request({ action: "acquire", name: "next", selector: thread }),
  ).toBe(false);
  await Promise.all([
    first.request({ action: "release", name: "first", selector: thread }),
    second.request({ action: "release", name: "next", selector: thread }),
  ]);
  expect(
    await second.request({
      action: "acquire",
      name: "still-unsafe",
      selector: thread,
    }),
  ).toBe(false);
  await second.request({
    action: "release",
    name: "still-unsafe",
    selector: thread,
  });
  await second.request({ action: "reset", selector: thread });
  expect(
    await second.request({
      action: "acquire",
      name: "recovered",
      selector: thread,
    }),
  ).toBe(true);
  await second.request({
    action: "release",
    name: "recovered",
    selector: thread,
  });
});

it("allows another thread and cancels a waiter without releasing the holder", async () => {
  const first = await worker();
  const second = await worker();
  await first.request({ action: "acquire", name: "first", selector: thread });
  expect(
    await second.request({ action: "acquire", name: "other", selector: other }),
  ).toBe(true);
  const waiting = second.start({
    action: "acquire",
    name: "cancelled",
    selector: thread,
    waitMs: 500,
  });
  await second.request({ action: "cancel", waitId: waiting.id });
  await expect(waiting.result).rejects.toThrow(/cancelled/);
  expect(
    await first.request({ action: "verify", name: "first", selector: thread }),
  ).toBe(true);
  await Promise.all([
    first.request({ action: "release", name: "first", selector: thread }),
    second.request({ action: "release", name: "other", selector: other }),
  ]);
});

it("poisons every process when a fallback writer has no selector", async () => {
  const first = await worker();
  const second = await worker();
  expect(
    await first.request({ action: "acquire", name: "first", selector: thread }),
  ).toBe(true);
  await second.request({ action: "nativeWrite" });
  expect(await first.request({ action: "verify", name: "first" })).toBe(false);
  await first.request({ action: "release", name: "first", selector: thread });
  expect(
    await second.request({ action: "acquire", name: "other", selector: other }),
  ).toBe(false);
  await second.request({ action: "release", name: "other", selector: other });
  await first.request({ action: "reset" });
  expect(
    await first.request({
      action: "acquire",
      name: "recovered",
      selector: other,
    }),
  ).toBe(true);
  await first.request({
    action: "release",
    name: "recovered",
    selector: other,
  });
});
