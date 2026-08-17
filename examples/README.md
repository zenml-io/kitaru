# Kitaru examples

## Complete returns workflow

Use the public [`zenml-io/kitaru-template`](https://github.com/zenml-io/kitaru-template) for the maintained product walkthrough. It provides a ready PydanticAI returns agent and checked-in Langfuse traces. Its root README owns setup and import; the [complete tutorial](../docs/book/tutorials/returns-agent/README.md) continues through deterministic diagnostics, evidence-linked human review, an immutable cohort version, and bounded replay.

## Standalone adapter examples

The adapter examples use packages from the independent plugin workspace:

- [`integrations/openai_agents_v2/`](integrations/openai_agents_v2/) uses `kitaru-openai-agents`.
- [`integrations/langgraph_v2/`](integrations/langgraph_v2/) uses `kitaru-langgraph`.

Install the workspace before running them:

```bash
uv sync --project plugins --frozen --all-packages
```

## Native v2 MCP configuration

[`v2/mcp/`](v2/mcp/) configures the native Kitaru MCP server in read-only mode. Use it after installing the `mcp` extra when a coding agent should inspect Kitaru registry and activity records without shelling out to the CLI.
