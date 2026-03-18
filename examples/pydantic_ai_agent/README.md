# PydanticAI adapter example

This directory shows how to wrap an existing PydanticAI agent and keep Kitaru's
durability and observability around it.

```bash
uv sync --extra local --extra pydantic-ai
uv run examples/pydantic_ai_agent/pydantic_ai_adapter.py
```

This example uses your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `uv run kitaru login ...`
(or `kitaru login ...`) and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

| Example | What it demonstrates | Test |
|---|---|---|
| [pydantic_ai_adapter.py](pydantic_ai_adapter.py) | `wrap(agent)` with captured child events, summaries, and a replay-safe outer checkpoint | [../../tests/test_phase17_pydantic_ai_example.py](../../tests/test_phase17_pydantic_ai_example.py) |

For the concept walkthrough, see
[PydanticAI Adapter](https://kitaru.ai/docs/getting-started/pydantic-ai-adapter).
