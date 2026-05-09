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

## Optional streaming run

To opt into the OpenAI Agents streaming path, set:

```bash
OPENAI_AGENTS_STREAM=1 uv run python openai_agents_adapter.py
```

That changes only the `runner_call` run: the flow calls
`KitaruRunner.run_stream_sync(...)` instead of `run_sync(...)`. The final value
is still the same `OpenAIRunResult` shape after the stream finishes.

Live terminal watching currently needs a streaming-enabled Kitaru/ZenML
backend. With the current Kitaru lock (`zenml==0.94.3`), local zero-config runs
still complete normally but do not expose live stream watching. For live events,
use either:

- a deployed/logged-in Kitaru server that includes ZenML streaming support, or
- a sibling ZenML `feature/streaming` checkout on `PYTHONPATH`, for local
  development:

```bash
PYTHONPATH=/path/to/zenml/src OPENAI_AGENTS_STREAM=1 uv run python openai_agents_adapter.py
```

In another terminal, once you have the execution ID, you can watch text deltas
with the low-level ZenML iterator:

```python
from zenml.client import Client

for event in Client().iter_run_events(
    "<execution-id>", kinds=["openai_agents.stream.event"]
):
    print(event.payload.get("text_delta", ""), end="", flush=True)
```

Streaming is intentionally narrow in this first version: it requires
`checkpoint_strategy="runner_call"`, must run inside a Kitaru flow, publishes
best-effort live progress events, and does not yet make partial token chunks
durable or replayable.

## What to look for in Kitaru UI

By default this example uses `checkpoint_strategy="runner_call"` — Kitaru
places one checkpoint around the whole `Runner.run(...)` call, so the
adapter's `OpenAIRunResult` becomes the flow's terminal artifact and
`flow.run(...).wait()` returns it cleanly.

## `calls` vs `runner_call` (plain-language version)

- `runner_call`: Kitaru places one bigger checkpoint around the entire
  `Runner.run(...)` call. Single terminal artifact, clean `.wait()` return.
- `calls`: Kitaru places smaller checkpoints around each supported model/tool
  call. Finer replay units, but each call becomes a peer checkpoint with no
  single sink — `.wait()` raises `KitaruAmbiguousFlowResultError` because
  there is no "the" return value. The per-checkpoint artifacts are still
  visible in the Kitaru UI.

To see both side-by-side (the default `runner_call` run, then the `calls`
run with the expected ambiguity error printed), use:

```bash
OPENAI_AGENTS_COMPARE_CALLS=1 uv run python openai_agents_adapter.py
```

In that mode you'll see the `runner_call` model output first, then a
`=== calls strategy output ===` section showing the new actionable error
that names the terminal checkpoints, gives the execution ID, and points at
the Kitaru UI / `KitaruClient` for per-checkpoint artifact retrieval.

## Why this example uses the real API

Our automated tests for the adapter use stubs to stay fast and deterministic.
This example is intentionally real API usage, so users can see authentic OpenAI
Agents SDK behavior plus Kitaru durability in practice.
