import { expect, it } from "vitest";
import { createProcessLocalMemoryAccess } from "../src/memory-binding.js";
import { RESOURCE, THREAD } from "./helpers/memory-agent.js";

it("shows why separate process-local helpers cannot qualify for multi-server replay", async () => {
  const first = createProcessLocalMemoryAccess();
  const second = createProcessLocalMemoryAccess();
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  const firstLease = await first.acquire(selector);
  const secondLease = await second.acquire(selector);
  expect(await firstLease.verifyEligibility()).toBe(true);
  expect(await secondLease.verifyEligibility()).toBe(true);
  await firstLease();
  await secondLease();
});

it("waits within waitMs for a releasing holder instead of invalidating it", async () => {
  const access = createProcessLocalMemoryAccess();
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  const first = await access.acquire(selector);
  setTimeout(() => void first(), 20);
  const second = await access.acquire(selector, { waitMs: 200 });
  expect(await second.verifyEligibility()).toBe(true);
  await second();
});
