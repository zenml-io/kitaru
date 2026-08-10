# Kitaru examples

## Canonical returns example

[`canonical_example/`](canonical_example/) is the main product walkthrough. It generates real PydanticAI and Langfuse traces for a synthetic returns agent, imports them into Kitaru, evaluates the baseline, creates cohorts, and replays an improved agent version.

Run every command from `examples/canonical_example` and follow its [README](canonical_example/README.md).

## Standalone adapter examples

The adapter examples use packages from the independent plugin workspace:

- [`canonical_example/`](canonical_example/) uses `kitaru-pydantic-ai`.
- [`integrations/openai_agents_v2/`](integrations/openai_agents_v2/) uses `kitaru-openai-agents`.
- [`integrations/langgraph_v2/`](integrations/langgraph_v2/) uses `kitaru-langgraph`.

Install the workspace before running them:

```bash
uv sync --project plugins --frozen --all-packages
```

## Native v2 MCP configuration

[`v2/mcp/`](v2/mcp/) configures the native Kitaru MCP server in read-only mode. Use it after installing the `mcp` extra when a coding agent should inspect Kitaru registry and activity records without shelling out to the CLI.
