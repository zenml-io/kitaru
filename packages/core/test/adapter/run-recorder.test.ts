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
      startedAt: "2026-01-01T00:00:00.500Z",
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
    expect(client.nodes[1]?.nodes[0]).toMatchObject({
      node_type: "llm_call",
      started_at: "2026-01-01T00:00:00.500Z",
    });
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

  it("records the bare provider family and keeps the qualified id", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await recordNormalizedStep(run.state, {
      attributes: { finish_reason: "stop" },
      failed: false,
      inputs: null,
      model: "gpt-5-nano-2026-01-01",
      outputs: { text: "done" },
      provider: "openai.responses",
      tools: [],
    });

    expect(client.nodes[1]?.nodes[0]).toMatchObject({
      attributes: { finish_reason: "stop", provider_id: "openai.responses" },
      model_provider: "openai",
      requested_model: "requested-model",
    });
  });

  it("records model settings supplied for one step", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await recordNormalizedStep(run.state, {
      attributes: {},
      failed: false,
      inputs: null,
      modelSettings: { temperature: 0.7 },
      outputs: { text: "done" },
      tools: [],
    });

    expect(client.nodes[1]?.nodes[0]?.model_params).toEqual({
      temperature: 0.7,
    });
  });

  it("omits the provider id when the adapter reports no provider", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await recordNormalizedStep(run.state, {
      attributes: {},
      failed: false,
      inputs: null,
      outputs: { text: "done" },
      tools: [],
    });

    const llmNode = client.nodes[1]?.nodes[0];
    expect(llmNode?.attributes).toEqual({});
    expect(llmNode?.model_provider).toBeUndefined();
  });

  it("records a step start when the adapter omits one", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await recordNormalizedStep(run.state, {
      attributes: {},
      endedAt: new Date(Date.now() + 1000).toISOString(),
      failed: false,
      inputs: { request: "body" },
      outputs: { text: "done" },
      tools: [],
    });

    const llmNode = client.nodes[1]?.nodes[0];
    expect(typeof llmNode?.started_at).toBe("string");
    expect(Date.parse(llmNode?.started_at ?? "")).toBeLessThanOrEqual(
      Date.parse(llmNode?.ended_at ?? ""),
    );
  });

  it("uses a completed ledger outcome when the framework omits the result", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    run.state.setToolCall({
      callId: "call-1",
      inputs: { value: "a" },
      mocked: false,
      outcome: "completed",
      output: { saved: true },
      toolName: "save",
    });

    await recordNormalizedStep(run.state, {
      attributes: {},
      failed: false,
      inputs: null,
      outputs: null,
      tools: [
        {
          callId: "call-1",
          inputs: { value: "a" },
          toolName: "save",
        },
      ],
    });

    expect(client.nodes[1]?.nodes[1]).toMatchObject({
      error: null,
      outputs: { saved: true },
      status: "completed",
    });
  });

  it("does not label an unintercepted tool call as failed", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();

    await recordNormalizedStep(run.state, {
      attributes: {},
      endedAt: "2026-01-01T00:00:01.000Z",
      failed: false,
      inputs: null,
      outputs: null,
      startedAt: "2026-01-01T00:00:00.000Z",
      tools: [
        {
          callId: "call-1",
          inputs: { value: "a" },
          toolName: "approval-gated",
        },
      ],
    });

    expect(client.nodes[1]?.nodes[1]).toMatchObject({
      error: null,
      outputs: null,
      status: "completed",
    });
    expect(client.nodes[1]?.nodes[1]?.started_at).toBeUndefined();
  });

  it("still fails an intercepted tool call that never completed", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    run.state.setToolCall({
      callId: "call-1",
      inputs: { value: "a" },
      mocked: false,
      outcome: "pending",
      toolName: "save",
    });

    await recordNormalizedStep(run.state, {
      attributes: {},
      failed: false,
      inputs: null,
      outputs: null,
      tools: [
        {
          callId: "call-1",
          inputs: { value: "a" },
          toolName: "save",
        },
      ],
    });

    expect(client.nodes[1]?.nodes[1]).toMatchObject({
      error: "Tool did not produce a result",
      outputs: null,
      status: "failed",
    });
  });

  it("does not hide an explicit framework tool failure with a completed ledger", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    run.state.setToolCall({
      callId: "call-1",
      inputs: {},
      mocked: true,
      outcome: "completed",
      output: { saved: true },
      policy: "static",
      toolName: "save",
    });

    await recordNormalizedStep(run.state, {
      attributes: {},
      failed: false,
      inputs: null,
      outputs: null,
      tools: [
        {
          callId: "call-1",
          inputs: {},
          result: {
            error: "Schema rejected output",
            failed: true,
            output: null,
          },
          toolName: "save",
        },
      ],
    });

    expect(client.nodes[1]?.nodes[1]).toMatchObject({
      error: "Schema rejected output",
      status: "failed",
    });
  });

  it("drains queued steps before marking the run failed", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    const pending = run.state.enqueueStep(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      await client.upsertSessionNodes(run.state.sessionId, {
        nodes: [
          {
            attributes: {},
            index: 1,
            inputs: null,
            name: "queued",
            node_type: "llm_call",
            outputs: null,
            parent_index: 0,
            status: "completed",
          },
        ],
      });
    });
    await run.fail(new Error("agent failed"));
    await pending;

    expect(client.nodes.map((batch) => batch.nodes[0]?.name)).toEqual([
      "run",
      "queued",
      "run",
    ]);
    expect(client.nodes.at(-1)?.nodes[0]).toMatchObject({ status: "failed" });
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
