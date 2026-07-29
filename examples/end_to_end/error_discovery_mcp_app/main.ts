import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import cors from "cors";
import type { Request, Response } from "express";

import { createServer } from "./server.js";
import { ReviewStore } from "./src/state.js";

async function startStdio(): Promise<void> {
  const store = new ReviewStore();
  await createServer(store).connect(new StdioServerTransport());
}

async function startHttp(): Promise<void> {
  const port = Number.parseInt(process.env.PORT ?? "3001", 10);
  const store = new ReviewStore();
  const app = createMcpExpressApp({ host: "127.0.0.1" });
  app.use(cors());

  app.all("/mcp", async (request: Request, response: Response) => {
    const server = createServer(store);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    response.on("close", () => {
      transport.close().catch(() => undefined);
      server.close().catch(() => undefined);
    });
    try {
      await server.connect(transport);
      await transport.handleRequest(request, response, request.body);
    } catch (error) {
      console.error(error);
      if (!response.headersSent) {
        response.status(500).json({
          jsonrpc: "2.0",
          error: { code: -32603, message: "Internal server error" },
          id: null,
        });
      }
    }
  });

  app.listen(port, "127.0.0.1", () => {
    console.log(`Kitaru error-discovery MCP server: http://127.0.0.1:${port}/mcp`);
  });
}

const run = process.argv.includes("--stdio") ? startStdio : startHttp;
run().catch((error) => {
  console.error(error);
  process.exit(1);
});
