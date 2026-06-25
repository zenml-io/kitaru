# fast-agent adapter example

This directory shows Kitaru wrapping a real
[fast-agent](https://github.com/evalstate/fast-agent) app while it calls
OpenAI's inexpensive `gpt-5-nano` model.

The example builds:

1. a real fast-agent `ToolAgent`,
2. a local Python `uppercase` tool,
3. a real OpenAI-backed fast-agent LLM, and
4. a `KitaruFastAgent` wrapper around the app run context.

When the flow runs, fast-agent still decides how to send messages and call the
tool. Kitaru wraps the app after `run()` yields, then records the reachable
`generate` and `call_tool` calls as Kitaru checkpoints.

## Getting started

The current preview extra is narrow: it installs the known-good
`fast-agent-mcp` version range and currently requires `Python >=3.13.5,<3.14`.

```bash
cd examples/integrations/fast_agent_agent
uv sync --extra fast-agent --no-dev
uv run kitaru init
export OPENAI_API_KEY='sk-...'
uv run python fast_agent_adapter.py
```

If you are running from the repository root instead, use:

```bash
uv sync --extra fast-agent --no-dev
uv run kitaru init
export OPENAI_API_KEY='sk-...'
uv run python examples/integrations/fast_agent_agent/fast_agent_adapter.py
```

The default model is `gpt-5-nano?reasoning=low`. Override it with
`FAST_AGENT_DEMO_MODEL` or `--model` if you want to test another fast-agent
OpenAI model spec.

For a no-network deterministic run, use:

```bash
uv run python fast_agent_adapter.py --provider memory
```

That memory path exists for local tests. The normal example uses OpenAI so the
checkpoint metadata can include real token counts and `genai-prices` cost
estimates.

## What to look for

The script prints a short summary:

```text
fast-agent adapter demo summary:
- provider: openai
- model: gpt-5-nano?reasoning=low
- model_reply: ...
- app_tool_reply: ...
- direct_tool_reply: REPLAY
```

In Kitaru, inspect the execution and look for checkpoints with names like:

- `fast_agent_demo_generate_model_call`
- `fast_agent_demo_uppercase_tool_call`

The model-call checkpoints should also include `llm_usage_v1` metadata. In the
default OpenAI run, the record should have provider/model information that
`genai-prices` can price, so the cost block should include an estimated USD
cost. If you run `--provider memory`, the usage numbers are local word counts
and the cost source is `none` because the fake memory model is not priceable.

The concrete story is:

1. The first `app.send(...)` asks the memory LLM for a normal answer, so Kitaru
   records a model-call checkpoint.
2. The second `app.send(...)` makes the memory LLM request the `uppercase` tool.
   In the tested fast-agent runtime, that normal app-driven tool path reaches
   `agent.call_tool`, so Kitaru records the tool call too.
3. The example also calls `agent.call_tool("uppercase", ...)` directly so the
   direct tool-call path is visible.

## Current preview limits

This adapter is an experimental preview because it depends on fast-agent runtime
object shapes that are not official hook APIs yet. Kitaru records calls that
pass through the app agents it can discover and wrap after `run()` yields:

- LLM `generate(...)`
- LLM `structured(...)` when the runtime object exposes it
- LLM `structured_schema(...)` when the runtime object exposes it
- `agent.call_tool(...)`
- detached agent clones returned by `spawn_detached_instance(...)`

Kitaru does not claim to record hidden fast-agent internals that never pass
through those reachable methods. Usage and cost tracking is also best-effort:
Kitaru records token usage when fast-agent exposes usage counts through an
accumulator or known result metadata, and it skips empty usage records when no
counts are available.

For the concept walkthrough, see
[fast-agent Adapter](https://docs.zenml.io/kitaru/adapters/fast-agent/).

For the full catalog, see [../../README.md](../../README.md).
