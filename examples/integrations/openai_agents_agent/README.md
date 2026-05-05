# OpenAI Agents adapter example (real API)

This example shows a real OpenAI Agents SDK customer-support flow inside Kitaru.

Story in one line: a customer asks about `ORD-1007`, the agent calls
`lookup_order`, then calls `shipping_policy`, and Kitaru records those calls as
checkpoints.

## Setup

```bash
cd examples/integrations/openai_agents_agent
uv sync --extra local --extra openai-agents
uv run kitaru init
export OPENAI_API_KEY='sk-...'
```

Default model is `gpt-5-nano`.

Optional model override (any OpenAI model you have access to):

```bash
export OPENAI_AGENTS_MODEL='<another-openai-model>'
```

## Run

```bash
uv run python openai_agents_adapter.py
```

If `OPENAI_API_KEY` is missing, the script exits early with a friendly message.

## What to look for in Kitaru UI

In `checkpoint_strategy="calls"` mode (default in this example), you should see
multiple checkpoints for one request, typically in a pattern like:

- model decides to call tool
- `lookup_order(...)`
- model decides to call tool
- `shipping_policy(...)`
- model writes final customer answer

That gives you a clear model/tool/model/tool/model durability story.

## `calls` vs `runner_call` (plain-language version)

- `calls`: Kitaru places smaller checkpoints around each supported model/tool
  call.
- `runner_call`: Kitaru places one bigger checkpoint around the entire
  `Runner.run(...)` call.

This example focuses on `calls` so the UI clearly shows each step.

If you want a side-by-side comparison, run with:

```bash
OPENAI_AGENTS_COMPARE_RUNNER_CALL=1 uv run python openai_agents_adapter.py
```

## Why this example uses the real API

Our automated tests for the adapter use stubs to stay fast and deterministic.
This example is intentionally real API usage, so users can see authentic OpenAI
Agents SDK behavior plus Kitaru durability in practice.
