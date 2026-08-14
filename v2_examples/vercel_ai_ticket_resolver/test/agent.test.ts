import { describe, expect, it } from "vitest";

import { createTicketRun, RESOLUTION_JSON_SCHEMA } from "../src/agent.js";
import { ticketCases } from "../src/fixtures.js";
import { renderTicketPrompt } from "../src/models.js";
import { SmokeClient } from "../src/smoke-client.js";

const AGENT_ID = "018f0000-0000-7000-8000-000000000100";

describe("Vercel returns resolver", () => {
  it("requires every strict structured-output property", () => {
    expect([...RESOLUTION_JSON_SCHEMA.required].sort()).toEqual(
      Object.keys(RESOLUTION_JSON_SCHEMA.properties).sort(),
    );
    expect(RESOLUTION_JSON_SCHEMA.properties.amount).toMatchObject({
      exclusiveMinimum: 0,
      type: ["number", "null"],
    });
  });

  it("loads the source-checkout adapter dist entrypoint", async () => {
    const adapter = await import("../../../packages/vercel-ai/dist/index.js");

    expect(adapter.createKitaruGenerateText).toBeTypeOf("function");
  });

  it("exposes exactly the canonical tools and records parseable output", async () => {
    const client = new SmokeClient();
    const ticket = ticketCases[0]?.ticket;
    if (!ticket) {
      throw new Error("ticket-001 fixture is missing");
    }

    const run = createTicketRun({
      client,
      environment: { KITARU_AGENT_ID: AGENT_ID },
      prompt: renderTicketPrompt(ticket),
    });
    const result = await run.generate();

    expect(Object.keys(run.tools)).toEqual([
      "lookup_order",
      "get_return_policy",
      "check_shipping",
      "issue_refund",
      "create_replacement",
      "escalate_to_human",
    ]);
    expect(JSON.parse(result.text)).toMatchObject({
      action: "refund",
      amount: 98,
    });
    expect(run.store.actions.filter(({ accepted }) => accepted)).toHaveLength(
      1,
    );
    expect(
      client.nodeBatches
        .flatMap(({ nodes }) => nodes)
        .filter(({ node_type }) => node_type === "tool_call")
        .map(({ tool_name }) => tool_name),
    ).toEqual(["lookup_order", "get_return_policy", "issue_refund"]);
    expect(client.created).toHaveLength(1);
    expect(client.updated.at(-1)?.status).toBe("completed");
    expect(client.updated.at(-1)?.outputs).toMatchObject({
      text: result.text,
    });
  });

  it.each(
    ticketCases.map(({ ticket }) => ticket),
  )("records the scripted baseline for $ticket_id", async (ticket) => {
    const run = createTicketRun({
      client: new SmokeClient(),
      environment: { KITARU_AGENT_ID: AGENT_ID },
      prompt: renderTicketPrompt(ticket),
    });

    const result = await run.generate();
    const resolution = JSON.parse(result.text) as { action: string };
    const accepted = run.store.actions.filter(({ accepted: ok }) => ok);

    expect(accepted).toHaveLength(1);
    expect(resolution.action).toBe(accepted[0]?.action);
  });

  it.each([
    "ticket-004",
    "ticket-007",
  ])("changes scripted target %s to escalation in strict mode", async (ticketId) => {
    const ticket = ticketCases.find(
      ({ ticket: candidate }) => candidate.ticket_id === ticketId,
    )?.ticket;
    if (!ticket) {
      throw new Error(`${ticketId} fixture is missing`);
    }

    const baseline = createTicketRun({
      client: new SmokeClient(),
      environment: { KITARU_AGENT_ID: AGENT_ID },
      mode: "baseline",
      prompt: renderTicketPrompt(ticket),
    });
    const strict = createTicketRun({
      client: new SmokeClient(),
      environment: { KITARU_AGENT_ID: AGENT_ID },
      mode: "strict",
      prompt: renderTicketPrompt(ticket),
    });

    expect(JSON.parse((await baseline.generate()).text).action).toBe("refund");
    expect(JSON.parse((await strict.generate()).text)).toMatchObject({
      action: "escalate",
      amount: null,
    });
    expect(strict.store.actions.some(({ action }) => action === "refund")).toBe(
      false,
    );
  });

  it.each([
    "ticket-001",
    "ticket-009",
    "ticket-010",
  ])("keeps scripted control %s as a capped refund in both modes", async (ticketId) => {
    const ticket = ticketCases.find(
      ({ ticket: candidate }) => candidate.ticket_id === ticketId,
    )?.ticket;
    if (!ticket) {
      throw new Error(`${ticketId} fixture is missing`);
    }
    for (const mode of ["baseline", "strict"] as const) {
      const run = createTicketRun({
        client: new SmokeClient(),
        environment: { KITARU_AGENT_ID: AGENT_ID },
        mode,
        prompt: renderTicketPrompt(ticket),
      });

      expect(JSON.parse((await run.generate()).text).action).toBe("refund");
      expect(run.store.actions.at(-1)).toMatchObject({
        accepted: true,
        action: "refund",
      });
    }
  });

  it("applies string worker task input in place of the default prompt", async () => {
    const ticket = ticketCases[9]?.ticket;
    if (!ticket) {
      throw new Error("ticket-010 fixture is missing");
    }
    const client = new SmokeClient();
    const replayPrompt = renderTicketPrompt(ticket);
    const run = createTicketRun({
      client,
      environment: {
        KITARU_AGENT_ID: AGENT_ID,
        KITARU_TASK_INPUTS: JSON.stringify(replayPrompt),
      },
      prompt: "Ticket ID: ticket-001\nThis default must be replaced.",
    });

    await run.generate();

    expect(client.created[0]?.inputs).toBe(replayPrompt);
  });

  it("rejects missing identity", () => {
    expect(() =>
      createTicketRun({
        environment: {},
        prompt: "Ticket ID: ticket-001",
      }),
    ).toThrow("KITARU_AGENT_ID is required");
  });

  it("fails an unknown deterministic ticket without guessing", async () => {
    const client = new SmokeClient();
    const run = createTicketRun({
      client,
      environment: { KITARU_AGENT_ID: AGENT_ID },
      prompt: "Ticket ID: ticket-999\nNo fixture exists for this ticket.",
    });

    await expect(run.generate()).rejects.toThrow(
      "deterministic model has no script for ticket-999",
    );
    expect(client.updated.at(-1)?.status).toBe("failed");
  });

  it("guards the optional paid provider before constructing a run", () => {
    expect(() =>
      createTicketRun({
        environment: {
          KITARU_AGENT_ID: AGENT_ID,
          OPENAI_API_KEY: "test-key",
        },
        provider: "openai",
        prompt: "Ticket ID: ticket-001",
      }),
    ).toThrow("RETURNS_ALLOW_PAID_MODEL=1");

    expect(() =>
      createTicketRun({
        environment: {
          KITARU_AGENT_ID: AGENT_ID,
          RETURNS_ALLOW_PAID_MODEL: "1",
        },
        provider: "openai",
        prompt: "Ticket ID: ticket-001",
      }),
    ).toThrow("OPENAI_API_KEY is required");
  });
});
