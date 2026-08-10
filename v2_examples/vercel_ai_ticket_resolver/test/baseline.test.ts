import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  type BaselineManifest,
  loadBaselineManifest,
  recordBaseline,
} from "../src/baseline.js";

const sessionId = (index: number) =>
  `018f0000-0000-7000-8000-${String(index).padStart(12, "0")}`;

async function stateDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "kitaru-vercel-returns-"));
}

describe("resumable baseline manifest", () => {
  it("records each successful ticket once and completes ten entries", async () => {
    const dir = await stateDir();
    const seen: string[] = [];

    const manifest = await recordBaseline({
      recordTicket: async ({ sessionIdFile, ticket }) => {
        seen.push(ticket.ticket_id);
        await writeFile(sessionIdFile, sessionId(seen.length), "utf8");
      },
      stateDir: dir,
    });

    expect(seen).toHaveLength(10);
    expect(Object.keys(manifest.sessions)).toHaveLength(10);
    expect(
      new Set(
        Object.values(manifest.sessions).map(({ session_id }) => session_id),
      ).size,
    ).toBe(10);
    expect(manifest.status).toBe("completed");
    expect(await loadBaselineManifest(dir)).toEqual(manifest);
  });

  it("resumes only missing tickets after an interruption", async () => {
    const dir = await stateDir();
    let attempts = 0;
    await expect(
      recordBaseline({
        recordTicket: async ({ sessionIdFile }) => {
          attempts += 1;
          if (attempts === 4) {
            throw new Error("fixture interruption before session creation");
          }
          await writeFile(sessionIdFile, sessionId(attempts), "utf8");
        },
        stateDir: dir,
      }),
    ).rejects.toThrow("fixture interruption");

    const resumed: string[] = [];
    const manifest = await recordBaseline({
      recordTicket: async ({ sessionIdFile, ticket }) => {
        resumed.push(ticket.ticket_id);
        await writeFile(sessionIdFile, sessionId(resumed.length + 100), "utf8");
      },
      stateDir: dir,
    });

    expect(resumed).toEqual([
      "ticket-004",
      "ticket-005",
      "ticket-006",
      "ticket-007",
      "ticket-008",
      "ticket-009",
      "ticket-010",
    ]);
    expect(manifest.status).toBe("completed");
  });

  it("rejects an uncommitted session-id file as ambiguous", async () => {
    const dir = await stateDir();
    const evidenceId = sessionId(700);
    const attemptDir = join(dir, "attempts", evidenceId);
    await mkdir(attemptDir, { recursive: true });
    const partial: BaselineManifest = {
      evidence_set_id: evidenceId,
      mode: "baseline",
      provider: "deterministic",
      schema_version: 1,
      sessions: {},
      status: "recording",
    };
    await writeFile(
      join(dir, "baseline-sessions.json"),
      JSON.stringify(partial),
      "utf8",
    );
    await writeFile(
      join(attemptDir, "ticket-001.session-id"),
      sessionId(200),
      "utf8",
    );

    await expect(
      recordBaseline({ recordTicket: async () => {}, stateDir: dir }),
    ).rejects.toThrow("Ambiguous attempt for ticket-001");
  });

  it("adopts only an explicitly matched ambiguous completed session", async () => {
    const dir = await stateDir();
    const adoptedId = sessionId(300);
    const evidenceId = sessionId(701);
    const attemptDir = join(dir, "attempts", evidenceId);
    await mkdir(attemptDir, { recursive: true });
    await writeFile(
      join(dir, "baseline-sessions.json"),
      JSON.stringify({
        evidence_set_id: evidenceId,
        mode: "baseline",
        provider: "deterministic",
        schema_version: 1,
        sessions: {},
        status: "recording",
      }),
      "utf8",
    );
    await writeFile(
      join(attemptDir, "ticket-001.session-id"),
      adoptedId,
      "utf8",
    );

    const recorded: string[] = [];
    const manifest = await recordBaseline({
      adoptions: { "ticket-001": adoptedId },
      recordTicket: async ({ sessionIdFile, ticket }) => {
        recorded.push(ticket.ticket_id);
        await writeFile(
          sessionIdFile,
          sessionId(recorded.length + 400),
          "utf8",
        );
      },
      stateDir: dir,
    });

    expect(manifest.sessions["ticket-001"]?.session_id).toBe(adoptedId);
    expect(recorded).not.toContain("ticket-001");
  });

  it("requires --fresh semantics before creating a second evidence set", async () => {
    const dir = await stateDir();
    let nextSession = 500;
    const recordTicket = async ({
      sessionIdFile,
    }: {
      sessionIdFile: string;
    }) => {
      nextSession += 1;
      await writeFile(sessionIdFile, sessionId(nextSession), "utf8");
    };
    const first = await recordBaseline({ recordTicket, stateDir: dir });
    const unchanged = await recordBaseline({ recordTicket, stateDir: dir });
    expect(unchanged.evidence_set_id).toBe(first.evidence_set_id);

    const second = await recordBaseline({
      fresh: true,
      recordTicket,
      stateDir: dir,
    });

    expect(second.evidence_set_id).not.toBe(first.evidence_set_id);
    expect(
      JSON.parse(
        await readFile(
          join(dir, "evidence-sets", `${first.evidence_set_id}.json`),
          "utf8",
        ),
      ),
    ).toEqual(first);
  });

  it("rejects an unapproved paid provider before creating manifest state", async () => {
    const dir = await stateDir();

    await expect(
      recordBaseline({
        environment: {
          KITARU_AGENT_ID: sessionId(900),
          OPENAI_API_KEY: "test-key",
        },
        provider: "openai",
        stateDir: dir,
      }),
    ).rejects.toThrow("RETURNS_ALLOW_PAID_MODEL=1");

    expect(await loadBaselineManifest(dir)).toBeUndefined();
  });

  it("rejects malformed state instead of guessing", async () => {
    const dir = await stateDir();
    await writeFile(
      join(dir, "baseline-sessions.json"),
      '{"schema_version":1,"sessions":{"ticket-001":{"session_id":"bad"}}}',
      "utf8",
    );

    await expect(loadBaselineManifest(dir)).rejects.toThrow(
      "baseline manifest",
    );
  });

  it("rejects a path-like evidence-set identifier", async () => {
    const dir = await stateDir();
    await writeFile(
      join(dir, "baseline-sessions.json"),
      JSON.stringify({
        evidence_set_id: "../outside-state",
        mode: "baseline",
        provider: "deterministic",
        schema_version: 1,
        sessions: {},
        status: "recording",
      }),
      "utf8",
    );

    await expect(loadBaselineManifest(dir)).rejects.toThrow(
      "baseline manifest",
    );
  });

  it("rejects a non-baseline manifest mode", async () => {
    const dir = await stateDir();
    await writeFile(
      join(dir, "baseline-sessions.json"),
      JSON.stringify({
        evidence_set_id: sessionId(702),
        mode: "strict",
        provider: "deterministic",
        schema_version: 1,
        sessions: {},
        status: "recording",
      }),
      "utf8",
    );

    await expect(loadBaselineManifest(dir)).rejects.toThrow(
      "baseline manifest",
    );
  });
});
