import { describe, expect, it, vi } from "vitest";

import { ExecutionTickets } from "../src/tickets.js";

describe("ExecutionTickets", () => {
  it("rejects a duplicate batch without registering any ticket", () => {
    const tickets = new ExecutionTickets();

    expect(() => tickets.register(["call-1", "call-1"])).toThrow(
      "Duplicate replay tool call ID 'call-1'",
    );
    expect(() => tickets.assertConsumed()).not.toThrow();
  });

  it("does not execute after an earlier adapter failure", async () => {
    const tickets = new ExecutionTickets();
    const execute = vi.fn(async () => "side effect");
    tickets.register(["call-1"]);

    await expect(
      tickets.run(
        "call-1",
        execute,
        undefined,
        () => true,
        () => true,
      ),
    ).rejects.toThrow("stopped after an adapter failure");
    expect(execute).not.toHaveBeenCalled();
  });
});
