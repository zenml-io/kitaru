import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import { projectRecordedInput } from "../src/options.js";
import { AGENT_ID, FakeClient, textResponse } from "./helpers.js";

describe("recording privacy", () => {
  it("redacts file, URL, and provider request values in input projections", () => {
    const projected = projectRecordedInput([
      {
        content: [
          { data: "FILE_SENTINEL", type: "file", url: "URL_SENTINEL" },
          { text: "kept", type: "text" },
        ],
        providerOptions: { secret: "PROVIDER_SENTINEL" },
        role: "user",
      },
    ]);
    const serialized = JSON.stringify(projected);
    expect(serialized).toContain("kept");
    expect(serialized).not.toContain("FILE_SENTINEL");
    expect(serialized).not.toContain("URL_SENTINEL");
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
});
