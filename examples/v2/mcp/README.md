# Kitaru v2 MCP server

This example configures Kitaru's native local MCP server in its default read-only mode. It contains no credentials and is safe to adapt for Claude Code or another client that accepts the same `mcpServers` JSON shape.

## Install

```bash
uv sync --extra mcp
```

Set `KITARU_API_URL` and, when required, `KITARU_API_KEY` in your private environment. Alternatively, pass `--server URL`; an existing credential stored for that exact URL can still be used. The MCP process selects its target and credential once at startup.

## Configure

Copy `.mcp.json.example` to the configuration location used by your client and replace the command with the absolute path to your environment's `kitaru-mcp` executable when it is not already on `PATH`.

The empty `args` array is deliberate: it keeps the server in `read-only` mode, which advertises only `kitaru_registry_read` and `kitaru_activity_read`. Review the [MCP server documentation](../../../docs/book/agent-native/setup.md) before adding `--mode standard` or `--mode destructive`.

Try asking your client to list one page of agents, inspect one session by UUID, or list one page of jobs. List operations return an opaque cursor instead of walking the complete collection.
