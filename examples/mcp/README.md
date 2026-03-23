# MCP example

This group is for assistant-native tooling: it shows how the Kitaru MCP server
can inspect executions and artifacts from an AI client like Claude Code or
Cursor.

## Getting started

```bash
cd examples/mcp
uv sync --extra local --extra mcp   # Install dependencies
kitaru init                          # Initialize a Kitaru project
python mcp_query_tools.py
```

This example uses your current Kitaru connection context. If you want MCP
queries to target a deployed Kitaru server, connect first with `kitaru login
<server>` and verify with `kitaru status`.

## `mcp_query_tools.py` — Query executions and artifacts through MCP

Demonstrates the three core MCP query tools: `kitaru_status()` for server
health, `kitaru_stacks_list()` for available infrastructure, and
`kitaru_executions_list()` for filtering executions by status. These are
the same tools an AI assistant uses when you add Kitaru as an MCP server —
this example just calls them directly as Python functions.

For the broader lifecycle story, see
[Execution Management](https://kitaru.ai/docs/getting-started/execution-management).

For the full catalog, see [../README.md](../README.md).
