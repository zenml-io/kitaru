import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const appDir = path.resolve(scriptDir, "..");
const transport = new StdioClientTransport({
  command: process.execPath,
  args: [path.join(appDir, "dist/main.js"), "--stdio"],
  stderr: "pipe",
});
transport.stderr?.on("data", (chunk) => process.stderr.write(chunk));

const client = new Client(
  { name: "error-discovery-stdio-smoke", version: "0.1.0" },
  { capabilities: {} },
);

try {
  await client.connect(transport);
  const tools = await client.listTools();
  const prompts = await client.listPrompts();
  const started = await client.callTool({
    name: "start_error_discovery",
    arguments: {},
  });
  const sessionId = started.structuredContent?.snapshot?.sessionId;
  if (
    tools.tools.length < 10 ||
    !prompts.prompts.some((prompt) => prompt.name === "error-discovery") ||
    typeof sessionId !== "string"
  ) {
    throw new Error("stdio smoke test did not receive expected MCP contracts");
  }
  console.log(
    `stdio smoke passed: ${tools.tools.length} tools, error-discovery prompt, session ${sessionId}`,
  );
} finally {
  await client.close();
}
