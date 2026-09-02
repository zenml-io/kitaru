---
description: Record runs of an agent that already reports to Langfuse, Braintrust, LangSmith, Logfire, or Arize Phoenix by importing each run's provider trace as a session
icon: cloud-arrow-down
---

# Importer-backed adapters

An importer-backed adapter records an agent that is already instrumented with an observability provider. It wraps the agent entrypoint in a provider trace, waits for the provider to finish ingesting that trace, fetches it, and imports it as one Kitaru session through the provider's importer. The agent framework stays untouched, and the provider stays your system of record.

Use one when the agent already reports to a supported provider and there is no [native adapter](README.md) for its framework. A native adapter records model and tool activity in process and can intercept it for [replay](../concepts/replay.md). An importer-backed adapter records after the fact from the provider's copy of the trace, so it cannot apply replay overrides or non-passthrough [tool policies](../guides/tool-policies.md).

## Available adapters

Each adapter ships inside the provider's importer package, behind the `adapter` extra. Installing the package without the extra gives you the importer alone.

| Provider | Install | Entry point | Credentials the trace fetch reads |
| --- | --- | --- | --- |
| Langfuse | `kitaru-langfuse-importer[adapter]` | `kitaru_langfuse_importer.LangfuseAdapter` | The Langfuse client configured in your process |
| Braintrust | `kitaru-braintrust-importer[adapter]` | `kitaru_braintrust_importer.BraintrustAdapter` | `BRAINTRUST_API_KEY` and the active Braintrust logger |
| LangSmith | `kitaru-langsmith-importer[adapter]` | `kitaru_langsmith_importer.LangSmithAdapter` | `LANGSMITH_API_KEY`, plus `LANGSMITH_ENDPOINT` for a self-hosted instance |
| Logfire | `kitaru-logfire-importer[adapter]` | `kitaru_logfire_importer.LogfireAdapter` | `LOGFIRE_TOKEN` for the SDK and `LOGFIRE_READ_TOKEN` for the fetch |
| Arize Phoenix | `kitaru-phoenix-importer[adapter]` | `kitaru_phoenix_importer.PhoenixAdapter` | `PHOENIX_ENDPOINT` or `PHOENIX_COLLECTOR_ENDPOINT`, `PHOENIX_API_KEY`, and `PHOENIX_PROJECT` |

## Record a run

Install the provider's importer package with the extra:

```bash
uv add "kitaru-langfuse-importer[adapter]"
```

Configure the provider SDK as you already do, then wrap the entrypoint:

```python
from kitaru_langfuse_importer import LangfuseAdapter


def run_agent(question: str) -> str:
    ...  # the Langfuse-instrumented agent


adapter = LangfuseAdapter()
result = adapter.run(run_agent, "What is an AI agent?")
```

`run` returns the function's own result. Use `await adapter.run_async(...)` for an async entrypoint. When the function raises, the adapter still imports the trace, so the failed run is recorded, and then re-raises.

The adapter only runs under a Kitaru worker task, which supplies the connection and the agent the session belongs to. Register the entrypoint as an agent version and start runs through the worker. The session is recorded with origin `recorded`, or `replay` under a replay, and names the provider as its import source.

## Register the agent version

Declare in the run spec that the runtime cannot apply overrides or tool policies:

```yaml
# version.yaml
run_spec:
  command: python agent.py
  runtime_capabilities:
    overrides: false
    tool_policies: false
```

```bash
kitaru agent version register support-agent --spec version.yaml
```

With that declaration, creating a replay or starting an experiment run against the version is rejected with HTTP 422 when the config carries an override or a non-passthrough tool policy. A passthrough replay runs the agent again for real and records the new run. See [runtime capabilities](../concepts/agents-and-sessions.md) for the declaration itself.

## Completeness timeout

The adapter polls the provider until the trace is complete, by default for up to 120 seconds, set with `completeness_timeout` on the adapter. When the trace does not complete in time, the adapter records a failed session that carries the provider trace id in its metadata and returns the function's result. The trace itself stays in the provider and can still be imported later.
