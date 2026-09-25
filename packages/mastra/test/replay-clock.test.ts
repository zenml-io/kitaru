import { expect, it } from "vitest";
import { createRecordedClock } from "../src/replay-clock.js";

const DAY_MS = 86_400_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

it("shifts Date.now only inside each recorded clock's own runs", async () => {
  const originalNow = Date.now;
  const started = Date.now();
  const weekAgo = createRecordedClock(new Date(started - 7 * DAY_MS));
  const monthAgo = createRecordedClock(new Date(started - 30 * DAY_MS));
  // Scheduled outside any run, so it fires while both runs are waiting.
  const outside = sleep(10).then(() => Date.now());
  const [week, month, unshifted] = await Promise.all([
    weekAgo.run(async () => {
      await sleep(20);
      return Date.now();
    }),
    monthAgo.run(async () => {
      await sleep(20);
      return Date.now();
    }),
    outside,
  ]);
  expect(Math.abs(started - 7 * DAY_MS - week)).toBeLessThan(1_000);
  expect(Math.abs(started - 30 * DAY_MS - month)).toBeLessThan(1_000);
  expect(Math.abs(unshifted - started)).toBeLessThan(1_000);
  expect(Math.abs(weekAgo.now().getTime() - week)).toBeLessThan(1_000);
  expect(Date.now).toBe(originalNow);
});
