import { App } from "@modelcontextprotocol/ext-apps";
import { EXTENSION_ID } from "@modelcontextprotocol/ext-apps/server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { createServer, resourceUri } from "../server.js";

describe("MCP Apps contracts", () => {
  let client: Client;
  let server: ReturnType<typeof createServer>;

  beforeEach(async () => {
    const [clientTransport, serverTransport] =
      InMemoryTransport.createLinkedPair();
    client = new Client(
      { name: "baby-vp-test-host", version: "0.1.0" },
      {
        capabilities: {
          extensions: {
            [EXTENSION_ID]: {
              mimeTypes: ["text/html;profile=mcp-app"],
            },
          },
        },
      },
    );
    server = createServer();
    await Promise.all([
      server.connect(serverTransport),
      client.connect(clientTransport),
    ]);
  });

  afterEach(async () => {
    await Promise.all([client.close(), server.close()]);
  });

  it("links the start tool to a stable MCP App resource", async () => {
    const tools = await client.listTools();
    const start = tools.tools.find(
      (tool) => tool.name === "start_error_discovery",
    );
    expect(start?._meta?.ui).toMatchObject({ resourceUri });

    const resource = await client.readResource({ uri: resourceUri });
    expect(resource.contents).toHaveLength(1);
    expect(resource.contents[0]?.mimeType).toBe(
      "text/html;profile=mcp-app",
    );
    expect(resource.contents[0]).toHaveProperty("text");
    expect((resource.contents[0] as { text: string }).text).toContain(
      "Kitaru error discovery",
    );
  });

  it("exposes the repository skill as an invocable MCP prompt", async () => {
    const prompts = await client.listPrompts();
    expect(prompts.prompts).toContainEqual(
      expect.objectContaining({
        name: "error-discovery",
        title: "Run trace-grounded error discovery",
      }),
    );
    const prompt = await client.getPrompt({ name: "error-discovery" });
    const text = prompt.messages[0]?.content;
    expect(text).toMatchObject({ type: "text" });
    expect((text as { text: string }).text).toContain(
      "The human notices and judges.",
    );
  });

  it("keeps mechanical actions app-only and transitions model-visible", async () => {
    const tools = await client.listTools();
    const byName = new Map(tools.tools.map((tool) => [tool.name, tool]));
    expect(byName.get("upsert_annotation")?._meta?.ui).toMatchObject({
      visibility: ["app"],
    });
    expect(byName.get("load_discovery_trace")?._meta?.ui).toMatchObject({
      visibility: ["app"],
    });
    expect(byName.get("record_scorer_run")?._meta?.ui).toMatchObject({
      visibility: ["model"],
    });
    expect(byName.get("reveal_validation_results")?._meta?.ui).toMatchObject({
      visibility: ["model"],
    });
    expect(byName.has("record_chat_confirmation")).toBe(false);
    expect(byName.get("confirm_failure_mode_draft")?._meta?.ui).toMatchObject({
      visibility: ["app"],
    });
    expect(byName.get("confirm_scorer_rubric_draft")?._meta?.ui).toMatchObject({
      visibility: ["app"],
    });
    expect(byName.get("record_scorer_run")?.inputSchema).toMatchObject({
      type: "object",
      required: expect.arrayContaining([
        "sessionId",
        "revision",
        "scorerId",
        "scorerHash",
        "predictions",
      ]),
    });
  });

  it("returns structured data and a useful text fallback", async () => {
    const started = await client.callTool({
      name: "start_error_discovery",
      arguments: {},
    });
    expect(started.structuredContent).toHaveProperty(
      "schemaVersion",
      "baby-vp.start.v1",
    );
    expect(started.structuredContent).toHaveProperty("snapshot.sessionId");
    const content = started.content as Array<{ type: string; text: string }>;
    expect(content[0]).toMatchObject({ type: "text" });
    expect(content[0]!.text).toContain(
      "Read backward",
    );
  });

  it("can construct the official App client used by the bundled UI", () => {
    expect(() => new App({ name: "contract-check", version: "0.1.0" })).not.toThrow();
  });
});
