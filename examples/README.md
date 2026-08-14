# Kitaru examples

## Canonical returns example

[`pydantic_ai_ticket_resolver/`](pydantic_ai_ticket_resolver/) is the main product walkthrough. It imports real PydanticAI and Langfuse traces, runs deterministic diagnostics, records evidence-linked human annotations and verdicts, freezes reviewed sessions into an immutable cohort version, and compares a candidate through replay.

Run every command from `examples/pydantic_ai_ticket_resolver` and follow its [README](pydantic_ai_ticket_resolver/README.md).

## Standalone adapter examples

The adapter examples use packages from the independent plugin workspace:

- [`pydantic_ai_ticket_resolver/`](pydantic_ai_ticket_resolver/) uses `kitaru-pydantic-ai`.
- [`integrations/openai_agents_v2/`](integrations/openai_agents_v2/) uses `kitaru-openai-agents`.
- [`integrations/langgraph_v2/`](integrations/langgraph_v2/) uses `kitaru-langgraph`.

Install the workspace before running them:

```bash
uv sync --project plugins --frozen --all-packages
```

## Native v2 MCP configuration

[`v2/mcp/`](v2/mcp/) configures the native Kitaru MCP server in read-only mode. Use it after installing the `mcp` extra when a coding agent should inspect Kitaru registry and activity records without shelling out to the CLI.
