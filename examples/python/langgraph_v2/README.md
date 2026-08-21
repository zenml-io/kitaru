# LangGraph v2 recording example

This provider-free example compiles a deterministic `StateGraph`, wraps it with `KitaruGraphRunner` from the `kitaru-langgraph` package, and calls `invoke()` once. Kitaru records one session with the graph input, output, and observable graph nodes. The graph itself only normalizes text and formats a response, so it needs no model provider or replay configuration.

## Set up and run

From the repository root:

```bash
uv sync --project plugins --all-packages

# Connect to a Kitaru v2 server if you have not already done so.
uv run kitaru login https://your-kitaru-server.example.com

# A direct local run must identify an existing agent or agent version.
export KITARU_AGENT_ID="..."
# Alternatively:
# export KITARU_AGENT_VERSION_ID="..."

uv run --project plugins python -m examples.python.langgraph_v2
```

The script prints:

```text
Recorded request: Reset my password
```

The Kitaru session records the original graph input, the completed graph output, and the public LangGraph callbacks observed during the invocation. A Kitaru worker supplies its task identity, inputs, replay identity, and credentials, so worker-managed runs do not need the local identity variables shown above.

This example intentionally stops at recording. Factory-built LangChain and Deep Agents can also apply Kitaru replay overrides and supported tool-result substitution; see the [LangGraph adapter guide](../../../docs/book/adapters/langgraph.md) for those capability boundaries.
