# Kitaru examples

## Canonical returns example

[`canonical_example/`](canonical_example/) is the main product walkthrough. It generates real PydanticAI and Langfuse traces for a synthetic returns agent, imports them into Kitaru, evaluates the baseline, creates cohorts, and replays an improved agent version.

Run every command from `examples/canonical_example` and follow its [README](canonical_example/README.md).

## Native v2 MCP configuration

[`v2/mcp/`](v2/mcp/) configures the native Kitaru MCP server in read-only mode. Use it after installing the `mcp` extra when a coding agent should inspect Kitaru registry and activity records without shelling out to the CLI.
