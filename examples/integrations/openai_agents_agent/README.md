# OpenAI Agents adapter example

This directory shows how to run an OpenAI Agents SDK `Agent` inside a Kitaru
flow using `KitaruRunner`.

## Getting started

```bash
cd examples/integrations/openai_agents_agent
uv sync --extra local --extra openai-agents
uv run kitaru init
```

Then run the example:

```bash
uv run python openai_agents_adapter.py
```

The example uses a tiny in-memory model implementation, so it runs without API
keys.

## `openai_agents_adapter.py` — Two checkpoint strategies

The script runs the same prompt twice with two `KitaruRunner` configurations:

- `checkpoint_strategy="calls"`: Kitaru catches supported model/tool calls
  individually.
- `checkpoint_strategy="runner_call"`: Kitaru places one checkpoint around the
  outer `Runner.run(...)` call.

For the concept walkthrough, see
[OpenAI Agents Adapter](https://kitaru.ai/docs/guides/openai-agents-adapter).
