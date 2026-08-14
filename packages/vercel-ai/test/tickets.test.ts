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

  it("lets a queued tool wait for a predecessor slower than the timeout", async () => {
    const tickets = new ExecutionTickets(20);
    tickets.register(["call-1", "call-2"]);

    const slow = tickets.run("call-1", async () => {
      await new Promise((resolve) => setTimeout(resolve, 60));
      return "slow";
    });
    const queued = tickets.run("call-2", async () => "queued");

    await expect(Promise.all([slow, queued])).resolves.toEqual([
      "slow",
      "queued",
    ]);
  });

  it("times out a queued tool whose predecessor never starts", async () => {
    const tickets = new ExecutionTickets(20);
    tickets.register(["call-1", "call-2"]);

    await expect(tickets.run("call-2", async () => "queued")).rejects.toThrow(
      "Replay tool ticket timed out after 20ms waiting to start",
    );
  });

  it("does not execute after an earlier adapter failure", async () => {
    const tickets = new ExecutionTickets();
    const execute = vi.fn(async () => "side effect");
    const failure = new Error("No static result for tool 'normalize'");
    tickets.register(["call-1"]);

    await expect(
      tickets.run(
        "call-1",
        execute,
        undefined,
        () => true,
        () => failure,
      ),
    ).rejects.toBe(failure);
    expect(execute).not.toHaveBeenCalled();
  });
});
