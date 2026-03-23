# PydanticAI adapter example

This directory shows how to wrap an existing PydanticAI agent and keep Kitaru's
durability and observability around it — no rewrite needed.

## Getting started

```bash
cd examples/pydantic_ai_agent
uv sync --extra local --extra pydantic-ai   # Install dependencies
kitaru init                                  # Initialize a Kitaru project
python pydantic_ai_adapter.py
```

This example uses your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `kitaru login <server>`
and verify with `kitaru status`.

## `pydantic_ai_adapter.py` — Wrap an agent, keep your replay boundary

Wraps a PydanticAI `Agent` with `kp.wrap(agent)`. The outer `@checkpoint`
becomes the replay boundary — if the flow is replayed, the entire agent
call is treated as a single unit. Internal model requests and tool calls
are tracked as child events under that checkpoint, giving you full
observability without changing the agent's control flow.

Uses `TestModel` so no API keys are needed to run it.

For the concept walkthrough, see
[PydanticAI Adapter](https://kitaru.ai/docs/getting-started/pydantic-ai-adapter).

For the full catalog, see [../README.md](../README.md).
