import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import { AGENT_ID, FakeAgent, installTestApi, textStep } from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("session lifecycle", () => {
  it("marks root and session failed while preserving an agent failure", async () => {
    const api = installTestApi();
    const original = new Error("model failed");
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.({
        ...textStep("error"),
        error: original,
        finishReason: "error",
      } as unknown as ReturnType<typeof textStep>);
      throw original;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    const error = await recorded.generate("fail").catch((caught) => caught);

    expect(error).toBe(original);
    expect(api.nodeBatches().at(-1)?.[0]).toMatchObject({
      error: "model failed",
      name: "run",
      status: "failed",
    });
    expect(api.calls.at(-1)?.body).toMatchObject({
      error: "model failed",
      status: "failed",
    });
  });

  it("surfaces terminal recording failure and attempts failed cleanup", async () => {
    const api = installTestApi();
    const originalFetch = globalThis.fetch;
    let nodeUpserts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/nodes") && init?.method === "POST") {
          nodeUpserts += 1;
          if (nodeUpserts === 3) {
            return new Response(JSON.stringify({ detail: "terminal failed" }), {
              headers: { "Content-Type": "application/json" },
              status: 500,
            });
          }
        }
        return originalFetch(input, init);
      }),
    );
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await expect(recorded.generate("run")).rejects.toThrow("terminal failed");

    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
  });

  it("keeps step recording fail-closed before user callbacks", async () => {
    const api = installTestApi();
    const originalFetch = globalThis.fetch;
    let nodeUpserts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/nodes") && init?.method === "POST") {
          nodeUpserts += 1;
          if (nodeUpserts === 2) {
            return new Response(JSON.stringify({ detail: "step failed" }), {
              headers: { "Content-Type": "application/json" },
              status: 500,
            });
          }
        }
        return originalFetch(input, init);
      }),
    );
    const configuredCallback = vi.fn();
    const callerCallback = vi.fn();
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredOnStepFinish: configuredCallback,
      requestedModelId: "requested-model",
    });

    await expect(
      recorded.generate("run", { onStepFinish: callerCallback }),
    ).rejects.toThrow("step failed");

    expect(configuredCallback).not.toHaveBeenCalled();
    expect(callerCallback).not.toHaveBeenCalled();
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
  });

  it("records a bounded summary of an otherwise unserializable result", async () => {
    const api = installTestApi();
    const circular: Record<string, unknown> = {
      finishReason: "stop",
      steps: [{}, {}],
      text: "final answer",
    };
    circular.self = circular;
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.(textStep());
      return circular;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    const result = await recorded.generate("run");

    expect(result).toBe(circular);
    expect(api.calls.at(-1)?.body).toMatchObject({
      outputs: {
        finish_reason: "stop",
        step_count: 2,
        text: "final answer",
      },
      status: "completed",
    });
  });

  it("does not replace an agent failure when cleanup also fails", async () => {
    installTestApi();
    const originalFetch = globalThis.fetch;
    let created = false;
    let nodeUpserts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof globalThis.fetch>(async (input, init) => {
        const url = new URL(String(input));
        if (url.pathname === "/v1/sessions") {
          created = true;
          return originalFetch(input, init);
        }
        if (created && url.pathname.endsWith("/nodes")) {
          nodeUpserts += 1;
          if (nodeUpserts > 1) {
            return new Response(JSON.stringify({ detail: "cleanup failed" }), {
              headers: { "Content-Type": "application/json" },
              status: 500,
            });
          }
        }
        if (created && init?.method === "PATCH") {
          return new Response(JSON.stringify({ detail: "cleanup failed" }), {
            headers: { "Content-Type": "application/json" },
            status: 500,
          });
        }
        return originalFetch(input, init);
      }),
    );
    const original = new Error("original agent failure");
    const agent = new FakeAgent(async () => {
      throw original;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    const error = await recorded.generate("run").catch((caught) => caught);

    expect(error).toBe(original);
  });

  it("fails the created session when the handoff file cannot be written", async () => {
    const api = installTestApi();
    vi.stubEnv(
      "KITARU_SESSION_ID_FILE",
      "/definitely/missing/kitaru/session-id.txt",
    );
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await expect(recorded.generate("run")).rejects.toThrow();

    expect(agent.calls).toHaveLength(0);
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "failed" });
  });
});
