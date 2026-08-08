import { describe, expect, it } from "vitest";
import type { AdapterClient } from "../../src/adapter/index.js";
import { RunRecorder, recordNormalizedStep } from "../../src/adapter/index.js";
import { fakeClient, SESSION_ID } from "./helpers.js";

async function recorder(client: AdapterClient): Promise<RunRecorder> {
  return RunRecorder.create({
    adapterVersion: "test-adapter",
    agentId: "018f0000-0000-7000-8000-000000000100",
    client,
    effectiveInput: { prompt: "hello" },
    framework: "test",
    requestedModelId: "requested-model",
    startedAt: "2026-01-01T00:00:00.000Z",
  });
}

describe("normalized run lifecycle", () => {
  it("creates, records, and completes one run", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await recordNormalizedStep(run.state, {
      attributes: {},
      endedAt: "2026-01-01T00:00:01.000Z",
      failed: false,
      inputs: { request: "body" },
      model: "effective-model",
      outputs: { text: "done" },
      provider: "test-provider",
      tools: [],
    });
    const result = { text: "done" };
    await run.complete(result);

    expect(client.created).toHaveLength(1);
    expect(client.created[0]).not.toHaveProperty("expected");
    expect(client.nodes.map((batch) => batch.nodes[0]?.node_type)).toEqual([
      "span",
      "llm_call",
      "span",
    ]);
    expect(client.nodes.at(-1)?.nodes[0]).toMatchObject({
      index: 0,
      outputs: result,
      status: "completed",
    });
    expect(client.updates.at(-1)).toMatchObject({
      outputs: result,
      status: "completed",
    });
  });

  it("creates no nodes when session creation fails", async () => {
    const client = fakeClient();
    client.createSession = async () => {
      throw new Error("create failed");
    };

    await expect(recorder(client)).rejects.toThrow("create failed");
    expect(client.nodes).toHaveLength(0);
    expect(client.updates).toHaveLength(0);
  });

  it("preserves the primary failure when cleanup also fails", async () => {
    const client = fakeClient({ throwOnNodes: true, throwOnUpdate: true });
    const run = await recorder(client);
    const primary = new Error("agent failed");

    await expect(run.fail(primary)).resolves.toBeUndefined();
    expect(run.state.failure).toBe(primary);
    expect(client.nodes).toHaveLength(1);
    expect(client.updates).toHaveLength(1);
    expect(run.state.sessionId).toBe(SESSION_ID);
  });
});
