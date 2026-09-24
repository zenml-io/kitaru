import { describe, expect, it } from "vitest";
import type { AdapterClient } from "../../src/adapter/index.js";
import { RunRecorder, recordNormalizedStep } from "../../src/adapter/index.js";
import { KitaruApiError } from "../../src/errors.js";
import { type FakeClient, fakeClient, SESSION_ID } from "./helpers.js";

/** Reject session updates that carry replay inputs, as an older server does. */
function rejectingFinalization(error: unknown): FakeClient {
  const client = fakeClient();
  const updateSession = client.updateSession;
  client.updateSession = async (sessionId, request) => {
    if ("inputs" in request) {
      client.updates.push(request);
      throw error;
    }
    return updateSession(sessionId, request);
  };
  return client;
}

const FINALIZATION = {
  inputs: { mastra_memory_replay: { version: 3, complete: true } },
  metadata: { mastra_replay_state: "eligible" },
};

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
  it("creates a pending recording with metadata atomically", async () => {
    const client = fakeClient();
    await RunRecorder.create({
      adapterVersion: "test-adapter",
      agentId: "018f0000-0000-7000-8000-000000000100",
      client,
      effectiveInput: { prompt: "hello" },
      framework: "mastra",
      metadata: { mastra_replay_state: "pending" },
      requestedModelId: "requested-model",
    });
    expect(client.created[0]?.metadata).toEqual({
      mastra_replay_state: "pending",
    });
  });

  it("publishes final inputs and eligibility with completion", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    const finalInputs = {
      mastra_memory_replay: { version: 3, complete: true },
    };
    await run.complete(
      { text: "done" },
      {
        inputs: finalInputs,
        metadata: {
          mastra_replay_state: "eligible",
          mastra_native_state: "completed",
        },
      },
    );
    expect(client.updates.at(-1)).toMatchObject({
      inputs: finalInputs,
      metadata: {
        mastra_replay_state: "eligible",
        mastra_native_state: "completed",
      },
      status: "completed",
    });
    expect(client.nodes.at(-1)?.nodes[0]?.inputs).toEqual(finalInputs);
  });

  it("keeps a completed run when the server rejects its replay inputs", async () => {
    const client = rejectingFinalization(
      new KitaruApiError(
        "PATCH",
        `/api/v1/sessions/${SESSION_ID}`,
        422,
        "Extra inputs are not permitted",
      ),
    );
    const run = await recorder(client);
    await run.initialize();

    const completion = await run.complete({ text: "done" }, FINALIZATION);

    expect(completion).toEqual({ finalizationAccepted: false });
    expect(client.updates).toHaveLength(2);
    expect(client.updates[0]).toMatchObject(FINALIZATION);
    expect(client.updates[1]).toEqual({
      ended_at: expect.any(String),
      outputs: { text: "done" },
      status: "completed",
    });
    expect(client.nodes.at(-1)?.nodes[0]).toMatchObject({
      outputs: { text: "done" },
      status: "completed",
    });
  });

  it("stores the rejected-finalization metadata when the server refuses replay inputs", async () => {
    const client = rejectingFinalization(
      new KitaruApiError(
        "PATCH",
        `/api/v1/sessions/${SESSION_ID}`,
        422,
        "Extra inputs are not permitted",
      ),
    );
    const run = await recorder(client);
    await run.initialize();
    const rejectedMetadata = {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "server_rejected_finalization",
    };

    const completion = await run.complete(
      { text: "done" },
      { ...FINALIZATION, rejectedMetadata },
    );

    expect(completion).toEqual({ finalizationAccepted: false });
    expect(client.updates[1]).toEqual({
      ended_at: expect.any(String),
      metadata: rejectedMetadata,
      outputs: { text: "done" },
      status: "completed",
    });
  });

  it("completes a finished run whose step uploads failed", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    await run.state
      .enqueueStep(async () => {
        throw new Error("step upload failed");
      })
      .catch(() => undefined);
    const metadata = {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "recording_step_failed",
    };

    await expect(run.complete({ text: "done" })).rejects.toThrow(
      "step upload failed",
    );
    await run.completeIncompleteRecording({ text: "done" }, metadata);

    expect(run.state.failure).toBeUndefined();
    expect(client.nodes.at(-1)?.nodes[0]).toMatchObject({
      outputs: { text: "done" },
      status: "completed",
    });
    expect(client.updates.at(-1)).toEqual({
      ended_at: expect.any(String),
      metadata,
      outputs: { text: "done" },
      status: "completed",
    });
  });

  it("does not retry a completion rejected for another reason", async () => {
    const conflict = new KitaruApiError(
      "PATCH",
      `/api/v1/sessions/${SESSION_ID}`,
      409,
      "Session does not accept updates",
    );
    const client = rejectingFinalization(conflict);
    const run = await recorder(client);
    await run.initialize();

    await expect(run.complete({ text: "done" }, FINALIZATION)).rejects.toBe(
      conflict,
    );
    expect(client.updates).toHaveLength(1);
  });

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
      external_id: "run",
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
            external_id: "queued",
            inputs: null,
            name: "queued",
            node_type: "llm_call",
            outputs: null,
            parent_external_id: "run",
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

  it("closes a failed recording without marking the application run failed", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.initialize();
    const recordingError = new Error("node write failed");

    await run.failRecording(recordingError);

    expect(run.state.failure).toBeUndefined();
    expect(client.nodes.at(-1)?.nodes[0]).toMatchObject({
      error: "node write failed",
      status: "failed",
    });
    expect(client.updates.at(-1)).toMatchObject({
      error: "node write failed",
      status: "failed",
    });
  });

  it("persists an ineligible recording reason in its terminal update", async () => {
    const client = fakeClient();
    const run = await recorder(client);
    await run.failRecording(new Error("capture failed"), {
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "capture_incomplete",
    });
    expect(client.updates.at(-1)?.metadata).toEqual({
      mastra_replay_state: "ineligible",
      mastra_replay_reason: "capture_incomplete",
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
