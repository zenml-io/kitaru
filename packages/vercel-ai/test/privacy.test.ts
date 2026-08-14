import {
  MAX_RECORDED_STRING_CHARS,
  projectRecordedInput,
  projectRecordedMetadata,
} from "@zenml-io/kitaru/adapter";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { MAX_WORKER_TASK_INPUT_CHARS } from "../src/options.js";
import { AGENT_ID, FakeClient, textResponse } from "./helpers.js";

describe("recording privacy", () => {
  it("rejects input values that cannot be recorded losslessly", () => {
    expect(() =>
      projectRecordedInput([
        {
          content: [
            { data: "FILE_SENTINEL", type: "file", url: "URL_SENTINEL" },
            { text: "kept", type: "text" },
          ],
          role: "user",
        },
      ]),
    ).toThrow("contains unsupported sensitive key 'data'");
  });

  it("bounds metadata without making it part of the replay contract", () => {
    const projected = projectRecordedMetadata({
      detail: "x".repeat(MAX_RECORDED_STRING_CHARS + 1),
      secret: "PROVIDER_SENTINEL",
    });
    const serialized = JSON.stringify(projected);
    expect(serialized).toContain("[truncated]");
    expect(serialized).not.toContain("PROVIDER_SENTINEL");
  });

  it("never records transport, provider, runtime, or request-body sentinels", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      apiKey: "AUTH_SENTINEL",
      client,
      environment: {},
    });

    await generate({
      experimental_include: { requestBody: true, requestMessages: true },
      headers: { "x-private": "HEADER_SENTINEL" },
      model,
      prompt: "safe prompt",
      providerOptions: { test: { secret: "PROVIDER_SENTINEL" } },
      runtimeContext: { secret: "RUNTIME_SENTINEL" },
    });

    const recorded = JSON.stringify({
      created: client.created,
      nodes: client.nodeBatches,
      updated: client.updated,
    });
    for (const sentinel of [
      "AUTH_SENTINEL",
      "HEADER_SENTINEL",
      "PROVIDER_SENTINEL",
      "RUNTIME_SENTINEL",
    ]) {
      expect(recorded).not.toContain(sentinel);
    }
    const llmNode = client.nodeBatches
      .flatMap((batch) => batch.nodes)
      .find((node) => node.node_type === "llm_call");
    expect(llmNode?.inputs).toBeNull();
  });

  it("rejects a lossy recorded input before model execution", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });

    await expect(
      generate({
        messages: [
          {
            content: [
              {
                data: "FILE_SENTINEL",
                mediaType: "text/plain",
                type: "file",
              },
            ],
            role: "user",
          },
        ],
        model,
      }),
    ).rejects.toThrow("contains unsupported sensitive key 'data'");
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it("records a caller prompt that is longer than the worker input bound", async () => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: {},
    });
    const prompt = "x".repeat(MAX_WORKER_TASK_INPUT_CHARS + 1);

    await generate({ model, prompt });

    expect(model.doGenerateCalls).toHaveLength(1);
    expect(client.created[0]?.inputs).toBe(prompt);
  });
});
