# PydanticAI adapter example

This directory shows how to wrap an existing PydanticAI agent and keep Kitaru's
durability and observability around it — no rewrite needed.

## Getting started

```bash
cd examples/integrations/pydantic_ai_agent
uv sync --extra local --extra pydantic-ai --extra openai
uv run kitaru init
```

Then run the no-key adapter example:

```bash
uv run python pydantic_ai_adapter.py       # wrap an agent, keep replay boundary
```

To watch real PydanticAI stream events, set provider credentials and run the
streaming example:

```bash
export OPENAI_API_KEY=sk-...
uv run python pydantic_ai_streaming.py
```

You can override the streaming model with `PYDANTIC_AI_MODEL`; it defaults to
`openai:gpt-5-nano`.

These examples use your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `kitaru login <server>`
and verify with `kitaru status`.

## `pydantic_ai_adapter.py` — Wrap an agent, keep your replay boundary

Wraps a PydanticAI `Agent` with `KitaruAgent(agent, ...)`. The outer
`@checkpoint` becomes the replay boundary — if the flow is replayed, the
entire agent call is treated as a single unit. Internal model requests
and tool calls are tracked as child events under that checkpoint, giving
you full observability without changing the agent's control flow.

Uses `TestModel` so no API keys are needed to run it.

## `pydantic_ai_streaming.py` — Watch live PydanticAI events

Runs a small customer-support agent against a real provider and watches
`pydantic_ai.stream.*` events with `KitaruClient().executions.events(...)` while
the flow is running. The live events are best-effort progress updates; the final
answer printed after `.wait()` is the durable Kitaru result.

This example requires `OPENAI_API_KEY` because it uses a real OpenAI-backed
PydanticAI model. Set `PYDANTIC_AI_MODEL` to choose a different PydanticAI model
label.

For the concept walkthrough, see
[PydanticAI Adapter](https://kitaru.ai/docs/guides/pydantic-ai-adapter).

For the full catalog, see [../../README.md](../../README.md).
