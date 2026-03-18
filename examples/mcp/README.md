# MCP example

This group is for assistant-native tooling: it shows how the Kitaru MCP server
can inspect executions and artifacts from an AI client.

```bash
uv sync --extra local --extra mcp
uv run examples/mcp/mcp_query_tools.py
```

This example uses your current Kitaru connection context. If you want MCP
queries to target a deployed Kitaru server, connect first with `uv run kitaru
login ...` (or `kitaru login ...`) and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

| Example | What it demonstrates | Test |
|---|---|---|
| [mcp_query_tools.py](mcp_query_tools.py) | Query executions and artifacts through the Kitaru MCP surface | [../../tests/mcp/test_phase19_mcp_example.py](../../tests/mcp/test_phase19_mcp_example.py) |

For the broader lifecycle story, see
[Execution Management](https://kitaru.ai/docs/getting-started/execution-management).
