# OpenAI Agents v2 adapter example

This example runs a real non-streaming OpenAI Agents SDK agent through Kitaru's source-only v2 adapter. The SDK returns its native `RunResult`; Kitaru records the run as one session with child observations.

## Check the example without a model call

From the repository root, sync and use the plugin workspace:

```bash
uv sync --project plugins
uv run --project plugins python -m examples.integrations.openai_agents_v2.agent --help
```

The help command imports the adapter and OpenAI SDK, then exits without reading credentials, creating a Kitaru session, or calling a model.

## Run it

Set OpenAI and Kitaru credentials. A standalone run also needs an agent or agent-version identity:

```bash
export OPENAI_API_KEY="..."
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="..."
export KITARU_AGENT_ID="..."
# Alternatively, set KITARU_AGENT_VERSION_ID instead of KITARU_AGENT_ID.

uv run --project plugins python -m examples.integrations.openai_agents_v2.agent \
  "Use the order lookup tool for ORD-1007"
```

A Kitaru worker supplies its own connection, task input, and `KITARU_TASK_ID`. Task-bound runs therefore do not need `KITARU_AGENT_ID` or `KITARU_AGENT_VERSION_ID`.

The example uses `KitaruRunner.run_sync(...)`. For asynchronous applications, await `KitaruRunner.run(...)` with the same agent and input.

The adapter does not support streaming, independently replayable observation nodes, sandbox helpers, custom result envelopes, or durable approval resume. Replay selection is worker-managed through `KITARU_REPLAY_ID`; the runner has no per-run replay argument. Use the Kitaru replay and task flow instead of mutating the process environment around concurrent standalone calls. See the [OpenAI Agents SDK guide](../../../docs/book/adapters/openai-agents.md) for the full boundary and payload-sensitivity warning.
