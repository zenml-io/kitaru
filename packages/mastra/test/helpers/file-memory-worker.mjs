import { createFileMemoryAccess } from "./file-memory-access.ts";

const access = createFileMemoryAccess(process.env.KITARU_LEASE_TEST_ROOT);
const leases = new Map();
const waits = new Map();

process.on("message", async (message) => {
  const { id, action, selector } = message;
  try {
    let result;
    if (action === "acquire") {
      const abort = new AbortController();
      waits.set(id, abort);
      try {
        const lease = await access.acquire(selector, {
          waitMs: message.waitMs,
          signal: abort.signal,
        });
        leases.set(message.name, lease);
        result = await lease.verifyEligibility();
      } finally {
        waits.delete(id);
      }
    } else if (action === "cancel") {
      waits.get(message.waitId)?.abort();
      result = true;
    } else if (action === "verify") {
      result = await leases.get(message.name).verifyEligibility();
    } else if (action === "release") {
      await leases.get(message.name)();
      leases.delete(message.name);
      result = true;
    } else if (action === "lose") {
      await access.simulateLeaseLoss(selector);
      result = true;
    } else if (action === "nativeWrite") {
      await access.markUnsafeWrite(selector);
      result = true;
    } else if (action === "reset") {
      await access.resetAfterQuiescence(selector);
      result = true;
    } else {
      throw new Error(`Unknown worker action: ${action}`);
    }
    process.send?.({ id, ok: true, result });
  } catch (error) {
    process.send?.({ id, ok: false, error: String(error) });
  }
});
